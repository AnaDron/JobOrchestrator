using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Single-threaded consumer событий оркестратора. Все state-transitions инстансов происходят здесь;
/// итерации запускаются на ThreadPool через <see cref="StageRunner"/>.
/// </summary>
/// <remarks>
/// <para>
/// Координация удаления инстансов: при <see cref="KeyRemovedEvent"/> аффектированные инстансы переезжают
/// в <c>_terminating</c> set и удаляются из <see cref="InstanceManager"/> сразу (чтобы новые триггеры
/// не находили их). Running-итерациям отменяется <c>RunCts</c>; <c>RemoveScopeAsync</c> откладывается
/// до момента, когда итерация физически завершилась (приходит <see cref="StageCompletedEvent"/> или
/// <see cref="StageFailedEvent"/>). События <see cref="KeyAddedEvent"/>/<see cref="KeyRemovedEvent"/>,
/// испущенные из terminating-инстанса, отфильтровываются по <c>Source</c>-полю.
/// </para>
/// <para>
/// Планирование Auto-тиков: <see cref="StageInstance.NextAutoUtc"/> хранит дедлайн, <see cref="DueScanner"/>
/// — централизованный pull-loop. После любого изменения расписания (создание инстанса, schedule next tick)
/// event loop вызывает <see cref="DueScanner.Wake"/> — scanner пересчитает ближайший due-момент.
/// </para>
/// </remarks>
internal sealed class EventLoop(
	StageRegistry registry,
	InstanceManager instances,
	KeyspaceRegistry keyspace,
	InstanceCreator creator,
	StageRunner runner,
	DueScanner scanner,
	Channel<OrchestratorEvent> channel,
	IJobStateStore stateStore,
	ILogger<EventLoop> logger,
	TimeProvider time
) {
	// Инстансы, для которых был запрошен cleanup, но running-итерация ещё не завершилась.
	// Cleanup завершается в обработчике StageCompleted/Failed для этого инстанса.
	private readonly HashSet<StageInstance> _terminating = [];

	public async Task RunAsync(CancellationToken stoppingToken) {
		logger.LogInformation("JobOrchestrator starting, stages={StageCount}", registry.AllStages.Count);
		BootstrapInitialInstances();
		// После bootstrap-а у DueScanner появляется работа — будим его.
		scanner.Wake();
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
		} finally {
			// Drain: оставшиеся в очереди запросы (Manual triggers) должны быть завершены
			// с Faulted/Exception, иначе их TaskCompletionSource'ы зависнут навсегда.
			DrainPendingRequests();
			// Финализируем все terminating-инстансы (их finalize-events потенциально не прилетят,
			// если runner-ы не успели опубликовать StageCompleted/Failed).
			await FinalizeAllTerminatingAsync().ConfigureAwait(false);
		}
		logger.LogInformation("JobOrchestrator stopped.");
	}

	private void DrainPendingRequests() {
		while (channel.Reader.TryRead(out var evt)) {
			if (evt is ManualTriggerRequestedEvent mt) {
				mt.Tcs.TrySetResult(TriggerResult.Faulted);
			}
			// TimerTicked/KeyAdded/KeyRemoved/StageCompleted/StageFailed без TCS — просто пропускаем.
		}
	}

	private async Task FinalizeAllTerminatingAsync() {
		if (_terminating.Count == 0) return;
		foreach (var instance in _terminating) {
			try {
				await stateStore.RemoveScopeAsync(instance.StateScope, CancellationToken.None).ConfigureAwait(false);
			} catch (Exception ex) {
				logger.LogWarning(ex, "Финализация terminating-инстанса {Instance} при shutdown завершилась с ошибкой.", instance.FullyQualifiedName);
			}
		}
		_terminating.Clear();
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
			case StageCompletedEvent sc: await HandleStageCompletedAsync(sc.Instance, sc.At, ct).ConfigureAwait(false); break;
			case StageFailedEvent sf: await HandleStageFailedAsync(sf.Instance, sf.Exception, sf.At, ct).ConfigureAwait(false); break;
			default:
				logger.LogWarning("Неизвестный тип события: {EventType}", evt.GetType().Name);
				break;
		}
	}

	private void HandleTimerTick(StageInstance instance, CancellationToken ct) {
		// Инстанс мог быть удалён (cascade) до подачи tick-а — проверим, что он всё ещё активен.
		if (instances.Find(instance.Stage.Name, instance.DependencyKeys) != instance) return;
		var now = time.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Auto, now);
		if (decision == TriggerResult.Started) {
			BeginIteration(instance, TriggerSource.Auto, ct);
		}
		// WaitingRetry/AlreadyRunning — ничего; следующий tick наступит, когда расписание обновится.
	}

	private void HandleManualTrigger(ManualTriggerRequestedEvent evt, CancellationToken ct) {
		var instance = instances.Find(evt.StageName, evt.DependencyKeys);
		if (instance is null) {
			evt.Tcs.TrySetResult(TriggerResult.NotFound);
			return;
		}
		var now = time.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Manual, now);
		evt.Tcs.TrySetResult(decision);
		if (decision == TriggerResult.Started) {
			BeginIteration(instance, TriggerSource.Manual, ct);
		}
	}

	private void BeginIteration(StageInstance instance, TriggerSource trigger, CancellationToken ct) {
		instance.State = InstanceLifecycleState.Running;
		// Снимаем NextAutoUtc, пока итерация запущена — DueScanner не должен пытаться запустить ещё одну.
		instance.NextAutoUtc = null;
		logger.LogDebug("BeginIteration {Instance} (trigger={Trigger})", instance.FullyQualifiedName, trigger);
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		instance.RunningTask = Task.Run(async () => await runner.RunIterationAsync(instance, trigger, ct).ConfigureAwait(false), ct);
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
		foreach (var dependent in registry.StagesDependingOnInstance(stageName)) {
			CreateAndStart(dependent);
		}
		scanner.Wake();
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

		var affectedStages = registry.StagesAffectedByKeyRemoval(stageName);

		var affected = affectedStages
			.SelectMany(s => instances.InstancesOf(s.Name))
			.Where(inst => inst.DependencyKeys.TryGetValue(stageName, out var v) && string.Equals(v, key, StringComparison.Ordinal))
			.ToList();
		if (affected.Count == 0) return;

		// Сортировка по pre-computed cancellation rank (листья — меньший ранг — отменяются первыми).
		affected.Sort((a, b) => registry.CancellationRank(a.Stage.Name).CompareTo(registry.CancellationRank(b.Stage.Name)));

		logger.LogDebug("RemoveKey({StageName},{Key}) каскадирует {Count} инстансов в порядке листьев→корней",
			stageName, key, affected.Count);

		foreach (var instance in affected) {
			// Извлекаем из active map (новые триггеры/lookup-ы больше не найдут).
			instances.Remove(instance);

			if (instance.State == InstanceLifecycleState.Running && instance.RunCts is { } cts) {
				// Сначала помечаем terminating (чтобы фильтровать события от ожидающей завершения итерации),
				// потом cancel. RemoveScopeAsync произойдёт в StageCompleted/Failed handler-е.
				_terminating.Add(instance);
				try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
				logger.LogDebug("Cascade-cancel running instance {Instance}; cleanup отложен до завершения итерации", instance.FullyQualifiedName);
			} else {
				logger.LogDebug("Cascade-remove idle instance {Instance}; RemoveScopeAsync синхронно", instance.FullyQualifiedName);
				try {
					await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
				} catch (Exception ex) {
					logger.LogWarning(ex, "RemoveScopeAsync для {Instance} завершился с ошибкой.", instance.FullyQualifiedName);
				}
			}
		}
		// После удаления, NextAutoUtc исчезнувших инстансов больше не считается — DueScanner пересчитает.
		scanner.Wake();
	}

	private async Task HandleStageCompletedAsync(StageInstance instance, DateTimeOffset at, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			logger.LogDebug("Terminating instance {Instance} закончил итерацию (success); финализируем cleanup", instance.FullyQualifiedName);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		bool wasFirstSuccess = !instance.LastSuccess.HasValue;
		instance.LastAttempt = at;
		instance.LastSuccess = at;       // монотонно: не сбрасывается на последующих неуспехах
		instance.ConsecutiveFailures = 0;
		instance.LastError = null;
		instance.State = InstanceLifecycleState.Idle;
		ScheduleNextTick(instance, instance.Stage.Interval);

		if (wasFirstSuccess) {
			logger.LogDebug("First success {Instance}; cascade зависимым стадиям", instance.FullyQualifiedName);
			foreach (var dependent in EnumerateDirectDependents(instance.Stage.Name)) {
				CreateAndStart(dependent);
			}
		}
		scanner.Wake();
	}

	private async Task HandleStageFailedAsync(StageInstance instance, Exception ex, DateTimeOffset at, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			logger.LogDebug("Terminating instance {Instance} закончил итерацию (failure); финализируем cleanup", instance.FullyQualifiedName);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		instance.LastAttempt = at;
		instance.ConsecutiveFailures++;
		instance.LastError = ex.Message;
		// LastSuccess НЕ меняется — монотонная метка.
		instance.State = InstanceLifecycleState.Idle;

		var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(instance.ConsecutiveFailures);
		var nextDelay = retryDelay > TimeSpan.Zero ? retryDelay : instance.Stage.Interval;
		ScheduleNextTick(instance, nextDelay);
		scanner.Wake();
	}

	private async Task FinalizeTerminatingAsync(StageInstance instance, CancellationToken ct) {
		try {
			await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			logger.LogWarning(ex, "RemoveScopeAsync для terminating-инстанса {Instance} завершился с ошибкой.", instance.FullyQualifiedName);
		}
	}

	private void ScheduleNextTick(StageInstance instance, TimeSpan delay) {
		instance.NextAutoUtc = time.GetUtcNow() + delay;
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = creator.EvaluateAndCreate(stage);
		foreach (var instance in created) {
			logger.LogDebug("Создан инстанс {Instance}", instance.FullyQualifiedName);
			// NextAutoUtc уже выставлен в InstanceCreator.MaterializeInstance как `now`,
			// поэтому DueScanner подберёт инстанс при ближайшем проходе.
		}
	}

	private IEnumerable<StageDescriptor> EnumerateDirectDependents(string stageName) =>
		registry.StagesDependingOn(stageName)
			.Concat(registry.StagesDependingOnInstance(stageName))
			.DistinctBy(s => s.Name, StringComparer.Ordinal);
}
