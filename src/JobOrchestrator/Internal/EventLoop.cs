using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Single-threaded consumer событий оркестратора. Все state-transitions инстансов происходят здесь;
/// итерации запускаются на ThreadPool через <see cref="StageRunner"/>.
/// </summary>
/// <remarks>
/// Координация удаления инстансов: при <see cref="KeyRemovedEvent"/> аффектированные инстансы переезжают
/// в <c>_terminating</c> set и удаляются из <see cref="InstanceManager"/> сразу (чтобы новые триггеры
/// не находили их). Running-итерациям отменяется <c>RunCts</c>; <c>RemoveScopeAsync</c> откладывается
/// до момента, когда итерация физически завершилась (приходит <see cref="StageCompletedEvent"/> или
/// <see cref="StageFailedEvent"/>). События <see cref="KeyAddedEvent"/>/<see cref="KeyRemovedEvent"/>,
/// испущенные из terminating-инстанса, отфильтровываются по <c>Source</c>-полю.
/// </remarks>
internal sealed class EventLoop(
	StageRegistry registry,
	InstanceManager instances,
	KeyspaceRegistry keyspace,
	InstanceCreator creator,
	StageRunner runner,
	Channel<OrchestratorEvent> channel,
	IJobStateStore stateStore,
	ILogger<EventLoop> logger,
	TimeProvider? timeProvider = null
) : IAsyncDisposable {
	private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
	private readonly Dictionary<StageInstance, InstanceTimer> _timers = [];
	// Инстансы, для которых был запрошен cleanup, но running-итерация ещё не завершилась.
	// Cleanup завершается в обработчике StageCompleted/Failed для этого инстанса.
	private readonly HashSet<StageInstance> _terminating = [];

	public async Task RunAsync(CancellationToken stoppingToken) {
		logger.LogInformation("JobOrchestrator starting, stages={StageCount}", registry.AllStages.Count);
		BootstrapInitialInstances();
		try {
			await foreach (var evt in channel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false)) {
				try {
					await HandleEventAsync(evt, stoppingToken).ConfigureAwait(false);
				} catch (Exception ex) {
					logger.LogCritical(ex, "Сбой обработчика события {EventType}", evt.GetType().Name);
				}
			}
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			// Нормальный shutdown.
		} catch (ChannelClosedException) {
			// Канал закрыт извне (lifecycle.MarkFaulted/CloseChannel). Нормальный shutdown.
		}
		logger.LogInformation("JobOrchestrator stopped.");
	}

	private void BootstrapInitialInstances() {
		foreach (var stage in registry.AllStages.Where(s => s.Dependencies.Count == 0))
			CreateAndStart(stage);
	}

	private async Task HandleEventAsync(OrchestratorEvent evt, CancellationToken ct) {
		switch (evt) {
			case TimerTickedEvent tt: HandleTimerTick(tt.Instance, ct); break;
			case ManualTriggerRequestedEvent mtr: HandleManualTrigger(mtr, ct); break;
			case KeyAddedEvent ka: HandleKeyAdded(ka.StageName, ka.Key, ka.Source); break;
			case KeyRemovedEvent kr: await HandleKeyRemovedAsync(kr.StageName, kr.Key, kr.Source, ct).ConfigureAwait(false); break;
			case StageCompletedEvent sc: await HandleStageCompletedAsync(sc.Instance, ct).ConfigureAwait(false); break;
			case StageFailedEvent sf: await HandleStageFailedAsync(sf.Instance, sf.Exception, ct).ConfigureAwait(false); break;
			case OverviewRequestedEvent or: HandleOverviewRequested(or); break;
			default:
				logger.LogWarning("Неизвестный тип события: {EventType}", evt.GetType().Name);
				break;
		}
	}

	private void HandleTimerTick(StageInstance instance, CancellationToken ct) {
		// Инстанс мог быть удалён до срабатывания timer'а — проверим существование.
		if (!_timers.ContainsKey(instance)) return;
		var now = _timeProvider.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Auto, now);
		if (decision == TriggerResult.Started)
			BeginIteration(instance, TriggerSource.Auto, ct);
		// WaitingRetry/AlreadyRunning — ничего; timer перепланируется в StageCompleted/Failed handler-е.
	}

	private void HandleManualTrigger(ManualTriggerRequestedEvent evt, CancellationToken ct) {
		var instance = instances.Find(evt.StageName, evt.DependencyKeys);
		if (instance is null) {
			evt.Tcs.TrySetResult(TriggerResult.NotFound);
			return;
		}
		var now = _timeProvider.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Manual, now);
		evt.Tcs.TrySetResult(decision);
		if (decision == TriggerResult.Started)
			BeginIteration(instance, TriggerSource.Manual, ct);
	}

	private void BeginIteration(StageInstance instance, TriggerSource trigger, CancellationToken ct) {
		instance.State = InstanceLifecycleState.Running;
		logger.LogDebug("BeginIteration {Instance} (trigger={Trigger})", instance.FullyQualifiedName, trigger);
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		_ = Task.Run(async () => await runner.RunIterationAsync(instance, trigger, ct).ConfigureAwait(false), ct);
	}

	private void HandleKeyAdded(string stageName, string key, StageInstance? source) {
		if (source is not null && _terminating.Contains(source)) {
			logger.LogDebug("Ignored AddKey({StageName},{Key}) from terminating instance {Instance}", stageName, key, source.FullyQualifiedName);
			return;
		}
		if (!registry.TryGet(stageName, out _)) {
			logger.LogWarning("AddKey({StageName},{Key}) — стадия не зарегистрирована в реестре, игнорируем", stageName, key);
			return;
		}
		if (!keyspace.Add(stageName, key)) {
			logger.LogDebug("AddKey({StageName},{Key}) — ключ уже в keyspace, no-op", stageName, key);
			return;
		}
		logger.LogDebug("AddKey({StageName},{Key}) — ключ добавлен; cascade зависимым стадиям", stageName, key);
		foreach (var dependent in registry.StagesDependingOnInstance(stageName))
			CreateAndStart(dependent);
	}

	private async Task HandleKeyRemovedAsync(string stageName, string key, StageInstance? source, CancellationToken ct) {
		if (source is not null && _terminating.Contains(source)) {
			logger.LogDebug("Ignored RemoveKey({StageName},{Key}) from terminating instance {Instance}", stageName, key, source.FullyQualifiedName);
			return;
		}
		if (!registry.TryGet(stageName, out _)) {
			logger.LogWarning("RemoveKey({StageName},{Key}) — стадия не зарегистрирована, игнорируем", stageName, key);
			return;
		}
		if (!keyspace.Remove(stageName, key)) {
			logger.LogDebug("RemoveKey({StageName},{Key}) — ключа не было, no-op", stageName, key);
			return;
		}

		// Транзитивное замыкание стадий, чьи инстансы могут содержать {stageName: key}.
		var affectedStages = registry.StagesAffectedByKeyRemoval(stageName);

		var affected = affectedStages
			.SelectMany(s => instances.InstancesOf(s.Name))
			.Where(inst => inst.DependencyKeys.TryGetValue(stageName, out var v) && string.Equals(v, key, StringComparison.Ordinal))
			.ToList();
		if (affected.Count == 0) return;

		// Топологически обратный порядок (листья перед корнями).
		var sortedStages = registry.TopologicalSortReverse(affectedStages);
		var stageOrderIndex = sortedStages
			.Select((s, i) => (s.Name, Index: i))
			.ToDictionary(t => t.Name, t => t.Index, StringComparer.Ordinal);
		affected.Sort((a, b) => stageOrderIndex[a.Stage.Name].CompareTo(stageOrderIndex[b.Stage.Name]));

		logger.LogDebug("RemoveKey({StageName},{Key}) каскадирует {Count} инстансов в порядке листьев→корней",
			stageName, key, affected.Count);

		foreach (var instance in affected) {
			// Извлекаем из active map (новые триггеры/lookup-ы больше не найдут).
			instances.Remove(instance);
			if (_timers.TryGetValue(instance, out var timer)) {
				timer.Dispose();
				_timers.Remove(instance);
			}

			if (instance.State == InstanceLifecycleState.Running && instance.RunCts is { } cts) {
				// Сначала помечаем terminating (чтобы фильтровать события от ожидающей завершения итерации),
				// потом cancel. RemoveScopeAsync произойдёт в StageCompleted/Failed handler-е.
				_terminating.Add(instance);
				try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
				logger.LogDebug("Cascade-cancel running instance {Instance}; cleanup отложен до завершения итерации", instance.FullyQualifiedName);
			} else {
				// Не running — можно очистить scope немедленно.
				logger.LogDebug("Cascade-remove idle instance {Instance}; RemoveScopeAsync синхронно", instance.FullyQualifiedName);
				try {
					await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
				} catch (Exception ex) {
					logger.LogWarning(ex, "RemoveScopeAsync для {Instance} завершился с ошибкой.", instance.FullyQualifiedName);
				}
			}
		}
	}

	private async Task HandleStageCompletedAsync(StageInstance instance, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			// Terminating-инстанс наконец завершил свою running-итерацию — теперь безопасно вычистить scope.
			logger.LogDebug("Terminating instance {Instance} закончил итерацию (success); финализируем cleanup", instance.FullyQualifiedName);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		bool wasFirstSuccess = !instance.LastSuccess.HasValue;
		var now = _timeProvider.GetUtcNow();
		instance.LastAttempt = now;
		instance.LastSuccess = now;       // монотонно: не сбрасывается на последующих неуспехах
		instance.ConsecutiveFailures = 0;
		instance.LastError = null;
		instance.State = InstanceLifecycleState.Idle;
		ScheduleNextTick(instance, instance.Stage.Interval);

		if (wasFirstSuccess) {
			logger.LogDebug("First success {Instance}; cascade зависимым стадиям", instance.FullyQualifiedName);
			foreach (var dependent in EnumerateDirectDependents(instance.Stage.Name))
				CreateAndStart(dependent);
		}
	}

	private async Task HandleStageFailedAsync(StageInstance instance, Exception ex, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			logger.LogDebug("Terminating instance {Instance} закончил итерацию (failure); финализируем cleanup", instance.FullyQualifiedName);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		var now = _timeProvider.GetUtcNow();
		instance.LastAttempt = now;
		instance.ConsecutiveFailures++;
		instance.LastError = ex.Message;
		// LastSuccess НЕ меняется — монотонная метка.
		instance.State = InstanceLifecycleState.Idle;

		var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(instance.ConsecutiveFailures);
		var nextDelay = retryDelay > TimeSpan.Zero ? retryDelay : instance.Stage.Interval;
		ScheduleNextTick(instance, nextDelay);
	}

	private async Task FinalizeTerminatingAsync(StageInstance instance, CancellationToken ct) {
		try {
			await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			logger.LogWarning(ex, "RemoveScopeAsync для terminating-инстанса {Instance} завершился с ошибкой.", instance.FullyQualifiedName);
		}
		// Timer уже dispose-ан в HandleKeyRemoved; инстанс уже Remove-нут из InstanceManager.
	}

	private void HandleOverviewRequested(OverviewRequestedEvent evt) {
		try {
			var overview = instances.ToOverview();
			evt.Tcs.TrySetResult(overview);
		} catch (Exception ex) {
			evt.Tcs.TrySetException(ex);
		}
	}

	private void ScheduleNextTick(StageInstance instance, TimeSpan delay) {
		instance.NextTickAtMs = Environment.TickCount64 + (long)delay.TotalMilliseconds;
		if (_timers.TryGetValue(instance, out var timer))
			timer.ScheduleAt(instance.NextTickAtMs);
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = creator.EvaluateAndCreate(stage);
		foreach (var instance in created) {
			logger.LogDebug("Создан инстанс {Instance}", instance.FullyQualifiedName);
			StartTimerAndScheduleImmediate(instance);
		}
	}

	private void StartTimerAndScheduleImmediate(StageInstance instance) {
		var timer = new InstanceTimer(instance, channel.Writer);
		_timers[instance] = timer;
		timer.ScheduleAt(Environment.TickCount64);
	}

	private IEnumerable<StageDescriptor> EnumerateDirectDependents(string stageName) =>
		registry.StagesDependingOn(stageName)
			.Concat(registry.StagesDependingOnInstance(stageName))
			.DistinctBy(s => s.Name, StringComparer.Ordinal);

	public ValueTask DisposeAsync() {
		foreach (var timer in _timers.Values) timer.Dispose();
		_timers.Clear();
		_terminating.Clear();
		return ValueTask.CompletedTask;
	}
}
