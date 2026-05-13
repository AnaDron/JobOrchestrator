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
/// <para>
/// Hot-path логи разворачиваются через <see cref="LoggerMessage.Define"/> (см. <see cref="Log"/>) —
/// zero-allocation для args[], предкэшированный formatter. Каждый вызов handler-а event-loop'а
/// может писать несколько log statements, в логе на DEBUG это десятки сообщений в секунду —
/// LoggerMessage даёт ощутимый allocation-cut.
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
		Log.Starting(logger, registry.AllStages.Count, null);
		BootstrapInitialInstances();
		// После bootstrap-а у DueScanner появляется работа — будим его.
		scanner.Wake();
		try {
			await foreach (var evt in channel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false)) {
				try {
					await HandleEventAsync(evt, stoppingToken).ConfigureAwait(false);
				} catch (Exception ex) {
					Log.HandlerCrashed(logger, evt.GetType().Name, ex);
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
		Log.Stopped(logger, null);
	}

	private void DrainPendingRequests() {
		while (channel.Reader.TryRead(out var evt)) {
			if (evt is ManualTriggerRequestedEvent mt) {
				mt.Tcs.TrySetResult(TriggerResult.Faulted);
			}
		}
	}

	private async Task FinalizeAllTerminatingAsync() {
		if (_terminating.Count == 0) return;
		foreach (var instance in _terminating) {
			try {
				await stateStore.RemoveScopeAsync(instance.StateScope, CancellationToken.None).ConfigureAwait(false);
			} catch (Exception ex) {
				Log.FinalizeShutdownFailed(logger, instance.FullyQualifiedName, ex);
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
				Log.UnknownEvent(logger, evt.GetType().Name, null);
				break;
		}
	}

	private void HandleTimerTick(StageInstance instance, CancellationToken ct) {
		// pendingTick освобождается ВСЕГДА при обработке tick-события — независимо от того,
		// запустим ли мы итерацию (Started) или отбросим (AlreadyRunning/WaitingRetry).
		// В случае Started — следующий due-tick будет после успешного перепланирования NextAutoUtc;
		// в случае AlreadyRunning — pendingTick освободит будущие due-окна, когда они появятся.
		instance.ReleasePendingTick();
		if (instances.Find(instance.Stage.Name, instance.DependencyKeys) != instance) return;
		var now = time.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Auto, now);
		if (decision == TriggerResult.Started) {
			BeginIteration(instance, TriggerSource.Auto, ct);
		}
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
		instance.NextAutoUtc = null;
		Log.BeginIteration(logger, instance.FullyQualifiedName, trigger, null);
		instance.RunningTask = Task.Run(async () => await runner.RunIterationAsync(instance, trigger, ct).ConfigureAwait(false), ct);
	}

	private void HandleKeyAdded(string stageName, string key, StageInstance? source) {
		if (source is not null && _terminating.Contains(source)) {
			Log.IgnoredAddKeyFromTerminating(logger, stageName, key, source.FullyQualifiedName, null);
			return;
		}
		if (!registry.TryGet(stageName, out _)) {
			Log.AddKeyUnknownStage(logger, stageName, key, null);
			return;
		}
		if (!keyspace.Add(stageName, key)) {
			Log.AddKeyAlreadyPresent(logger, stageName, key, null);
			return;
		}
		Log.AddKeyAdded(logger, stageName, key, null);
		foreach (var dependent in registry.StagesDependingOnInstance(stageName)) {
			CreateAndStart(dependent);
		}
		scanner.Wake();
	}

	private async Task HandleKeyRemovedAsync(string stageName, string key, StageInstance? source, CancellationToken ct) {
		if (source is not null && _terminating.Contains(source)) {
			Log.IgnoredRemoveKeyFromTerminating(logger, stageName, key, source.FullyQualifiedName, null);
			return;
		}
		if (!registry.TryGet(stageName, out _)) {
			Log.RemoveKeyUnknownStage(logger, stageName, key, null);
			return;
		}
		if (!keyspace.Remove(stageName, key)) {
			Log.RemoveKeyAbsent(logger, stageName, key, null);
			return;
		}

		var affectedStages = registry.StagesAffectedByKeyRemoval(stageName);
		var affected = affectedStages
			.SelectMany(s => instances.InstancesOf(s.Name))
			.Where(inst => inst.DependencyKeys.TryGetValue(stageName, out var v) && string.Equals(v, key, StringComparison.Ordinal))
			.ToList();
		if (affected.Count == 0) return;

		affected.Sort((a, b) => registry.CancellationRank(a.Stage.Name).CompareTo(registry.CancellationRank(b.Stage.Name)));
		Log.RemoveKeyCascade(logger, stageName, key, affected.Count, null);

		foreach (var instance in affected) {
			instances.Remove(instance);

			if (instance.State == InstanceLifecycleState.Running && instance.RunCts is { } cts) {
				_terminating.Add(instance);
				try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
				Log.CascadeCancelRunning(logger, instance.FullyQualifiedName, null);
			} else {
				Log.CascadeRemoveIdle(logger, instance.FullyQualifiedName, null);
				try {
					await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
				} catch (Exception ex) {
					Log.RemoveScopeFailed(logger, instance.FullyQualifiedName, ex);
				}
			}
		}
		scanner.Wake();
	}

	private async Task HandleStageCompletedAsync(StageInstance instance, DateTimeOffset at, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			Log.TerminatingCompletedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		bool wasFirstSuccess = !instance.LastSuccess.HasValue;
		instance.LastAttempt = at;
		instance.LastSuccess = at;
		instance.ConsecutiveFailures = 0;
		instance.LastError = null;
		instance.State = InstanceLifecycleState.Idle;
		ScheduleNextTick(instance, instance.Stage.Interval);

		if (wasFirstSuccess) {
			Log.FirstSuccessCascade(logger, instance.FullyQualifiedName, null);
			foreach (var dependent in EnumerateDirectDependents(instance.Stage.Name)) {
				CreateAndStart(dependent);
			}
		}
		scanner.Wake();
	}

	private async Task HandleStageFailedAsync(StageInstance instance, Exception ex, DateTimeOffset at, CancellationToken ct) {
		if (_terminating.Remove(instance)) {
			Log.TerminatingFailedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}

		instance.LastAttempt = at;
		instance.ConsecutiveFailures++;
		instance.LastError = ex.Message;
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
			Log.FinalizeFailed(logger, instance.FullyQualifiedName, ex);
		}
	}

	private void ScheduleNextTick(StageInstance instance, TimeSpan delay) {
		instance.NextAutoUtc = time.GetUtcNow() + delay;
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = creator.EvaluateAndCreate(stage);
		foreach (var instance in created) {
			Log.InstanceCreated(logger, instance.FullyQualifiedName, null);
		}
	}

	private IEnumerable<StageDescriptor> EnumerateDirectDependents(string stageName) =>
		registry.StagesDependingOn(stageName)
			.Concat(registry.StagesDependingOnInstance(stageName))
			.DistinctBy(s => s.Name, StringComparer.Ordinal);

	/// <summary>
	/// Pre-allocated <see cref="LoggerMessage.Define{T}"/>-делегаты для всех hot-path логов EventLoop.
	/// EventId-ы 3xxx — диапазон EventLoop. Преимущество vs обычный <c>logger.LogXxx</c>:
	/// ноль аллокаций <c>object[]</c> на args + закэшированный formatter (~3-5x перформанс на DEBUG-spam).
	/// </summary>
	private static class Log {
		public static readonly Action<ILogger, int, Exception?> Starting =
			LoggerMessage.Define<int>(LogLevel.Information, new EventId(3001, nameof(Starting)),
				"JobOrchestrator starting, stages={StageCount}");

		public static readonly Action<ILogger, Exception?> Stopped =
			LoggerMessage.Define(LogLevel.Information, new EventId(3002, nameof(Stopped)),
				"JobOrchestrator stopped.");

		public static readonly Action<ILogger, string, Exception?> HandlerCrashed =
			LoggerMessage.Define<string>(LogLevel.Critical, new EventId(3003, nameof(HandlerCrashed)),
				"Сбой обработчика события {EventType}");

		public static readonly Action<ILogger, string, Exception?> UnknownEvent =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(3004, nameof(UnknownEvent)),
				"Неизвестный тип события: {EventType}");

		public static readonly Action<ILogger, string, Exception?> FinalizeShutdownFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(3005, nameof(FinalizeShutdownFailed)),
				"Финализация terminating-инстанса {Instance} при shutdown завершилась с ошибкой.");

		public static readonly Action<ILogger, string, TriggerSource, Exception?> BeginIteration =
			LoggerMessage.Define<string, TriggerSource>(LogLevel.Debug, new EventId(3006, nameof(BeginIteration)),
				"BeginIteration {Instance} (trigger={Trigger})");

		public static readonly Action<ILogger, string, string, string, Exception?> IgnoredAddKeyFromTerminating =
			LoggerMessage.Define<string, string, string>(LogLevel.Debug, new EventId(3007, nameof(IgnoredAddKeyFromTerminating)),
				"Ignored AddKey({StageName},{Key}) from terminating instance {Instance}");

		public static readonly Action<ILogger, string, string, Exception?> AddKeyUnknownStage =
			LoggerMessage.Define<string, string>(LogLevel.Warning, new EventId(3008, nameof(AddKeyUnknownStage)),
				"AddKey({StageName},{Key}) — стадия не зарегистрирована в реестре, игнорируем");

		public static readonly Action<ILogger, string, string, Exception?> AddKeyAlreadyPresent =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3009, nameof(AddKeyAlreadyPresent)),
				"AddKey({StageName},{Key}) — ключ уже в keyspace, no-op");

		public static readonly Action<ILogger, string, string, Exception?> AddKeyAdded =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3010, nameof(AddKeyAdded)),
				"AddKey({StageName},{Key}) — ключ добавлен; cascade зависимым стадиям");

		public static readonly Action<ILogger, string, string, string, Exception?> IgnoredRemoveKeyFromTerminating =
			LoggerMessage.Define<string, string, string>(LogLevel.Debug, new EventId(3011, nameof(IgnoredRemoveKeyFromTerminating)),
				"Ignored RemoveKey({StageName},{Key}) from terminating instance {Instance}");

		public static readonly Action<ILogger, string, string, Exception?> RemoveKeyUnknownStage =
			LoggerMessage.Define<string, string>(LogLevel.Warning, new EventId(3012, nameof(RemoveKeyUnknownStage)),
				"RemoveKey({StageName},{Key}) — стадия не зарегистрирована, игнорируем");

		public static readonly Action<ILogger, string, string, Exception?> RemoveKeyAbsent =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3013, nameof(RemoveKeyAbsent)),
				"RemoveKey({StageName},{Key}) — ключа не было, no-op");

		public static readonly Action<ILogger, string, string, int, Exception?> RemoveKeyCascade =
			LoggerMessage.Define<string, string, int>(LogLevel.Debug, new EventId(3014, nameof(RemoveKeyCascade)),
				"RemoveKey({StageName},{Key}) каскадирует {Count} инстансов в порядке листьев→корней");

		public static readonly Action<ILogger, string, Exception?> CascadeCancelRunning =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3015, nameof(CascadeCancelRunning)),
				"Cascade-cancel running instance {Instance}; cleanup отложен до завершения итерации");

		public static readonly Action<ILogger, string, Exception?> CascadeRemoveIdle =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3016, nameof(CascadeRemoveIdle)),
				"Cascade-remove idle instance {Instance}; RemoveScopeAsync синхронно");

		public static readonly Action<ILogger, string, Exception?> RemoveScopeFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(3017, nameof(RemoveScopeFailed)),
				"RemoveScopeAsync для {Instance} завершился с ошибкой.");

		public static readonly Action<ILogger, string, Exception?> TerminatingCompletedFinalize =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3018, nameof(TerminatingCompletedFinalize)),
				"Terminating instance {Instance} закончил итерацию (success); финализируем cleanup");

		public static readonly Action<ILogger, string, Exception?> FirstSuccessCascade =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3019, nameof(FirstSuccessCascade)),
				"First success {Instance}; cascade зависимым стадиям");

		public static readonly Action<ILogger, string, Exception?> TerminatingFailedFinalize =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3020, nameof(TerminatingFailedFinalize)),
				"Terminating instance {Instance} закончил итерацию (failure); финализируем cleanup");

		public static readonly Action<ILogger, string, Exception?> FinalizeFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(3021, nameof(FinalizeFailed)),
				"RemoveScopeAsync для terminating-инстанса {Instance} завершился с ошибкой.");

		public static readonly Action<ILogger, string, Exception?> InstanceCreated =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3022, nameof(InstanceCreated)),
				"Создан инстанс {Instance}");
	}
}
