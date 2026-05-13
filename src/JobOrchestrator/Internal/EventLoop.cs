using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Single-threaded consumer событий оркестратора. Все state-transitions инстансов происходят здесь;
/// итерации запускаются на ThreadPool через <see cref="StageRunner"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Terminating state.</b> При <see cref="KeyRemovedEvent"/> аффектированные инстансы получают
/// <see cref="InstanceLifecycleState.Terminating"/>. Они остаются в <see cref="InstanceManager"/>
/// до фактического finalize'а, но новые триггеры (<see cref="TriggerAcceptance"/>) и DueScanner
/// игнорируют их (state != Idle). Cleanup для Running-инстансов откладывается до их
/// <see cref="StageCompletedEvent"/>/<see cref="StageFailedEvent"/>; для Idle-инстансов — синхронно
/// в момент cascade.
/// </para>
/// <para>
/// <b>Iterative cascade.</b> Каскад при <see cref="KeyRemovedEvent"/> обходит транзитивное замыкание
/// через очередь (BFS), без рекурсивных await-frame'ов — защищает стек от deep-graph-cascade-storm.
/// </para>
/// <para>
/// <b>Schedule next-tick от <c>at</c>.</b> NextAutoUtc вычисляется от <c>at</c> (фактическое время
/// завершения runner-а), а не от <c>time.GetUtcNow()</c> в момент обработки события — это сохраняет
/// корректное расписание даже когда event-loop отстаёт.
/// </para>
/// <para>
/// <b>Try/finally state consistency.</b> В StageCompleted/Failed handler-е <see cref="StageInstance.State"/>=Idle
/// проставляется в finally — exception между SetMetrics и State=Idle не оставит инстанс залипшим в Running.
/// </para>
/// </remarks>
internal sealed class EventLoop(
	StageRegistry registry,
	InstanceManager instances,
	KeyspaceRegistry keyspace,
	InstanceCreator creator,
	StageRunner runner,
	DueScanner scanner,
	ConcurrencyLimits concurrency,
	Channel<OrchestratorEvent> channel,
	IJobStateStore stateStore,
	ILogger<EventLoop> logger,
	TimeProvider time
) {
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
			DrainPendingRequests();
			// Финализируем все Terminating-инстансы (их finalize-events не прилетят при закрытом Channel).
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
		// Снимок терминирующих инстансов через State-чтение; их StageCompleted/Failed уже не придут.
		var terminating = instances.All
			.Where(inst => inst.State == InstanceLifecycleState.Terminating)
			.ToList();
		foreach (var instance in terminating) {
			try {
				await stateStore.RemoveScopeAsync(instance.StateScope, CancellationToken.None).ConfigureAwait(false);
			} catch (Exception ex) {
				Log.FinalizeShutdownFailed(logger, instance.FullyQualifiedName, ex);
			}
			instances.Remove(instance);
		}
	}

	private void BootstrapInitialInstances() {
		foreach (var stage in registry.AllStages.Where(s => s.Dependencies.Count == 0))
			CreateAndStart(stage);
	}

	private async Task HandleEventAsync(OrchestratorEvent evt, CancellationToken ct) {
		switch (evt) {
			case TimerTickedEvent tt: HandleTimerTick(tt.Instance, ct); break;
			case ManualTriggerRequestedEvent mtr: HandleManualTrigger(mtr, ct); break;
			case KeyAddedEvent ka: HandleKeyAdded(ka.Source, ka.Key); break;
			case KeyRemovedEvent kr: await HandleKeyRemovedAsync(kr.Source, kr.Key, ct).ConfigureAwait(false); break;
			case StageCompletedEvent sc: await HandleStageCompletedAsync(sc.Instance, sc.At, ct).ConfigureAwait(false); break;
			case StageFailedEvent sf: await HandleStageFailedAsync(sf.Instance, sf.Exception, sf.At, ct).ConfigureAwait(false); break;
			default:
				Log.UnknownEvent(logger, evt.GetType().Name, null);
				break;
		}
	}

	private void HandleTimerTick(StageInstance instance, CancellationToken ct) {
		// pendingTick освобождается ВСЕГДА при обработке tick-события.
		instance.ReleasePendingTick();
		// Идемпотентность: инстанс мог быть уже Terminated/удалён.
		if (instances.Find(instance.Identity) != instance) return;
		if (instance.State != InstanceLifecycleState.Idle) return;
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
		// ConcurrencyLimit: если стадия уже на лимите — re-schedule инстанс через короткое окно
		// (1 sec), не меняя State (остаётся Idle). DueScanner подберёт его снова, когда лимит откроется.
		if (!concurrency.TryAcquire(instance.Stage.Name)) {
			Log.ConcurrencyDeferred(logger, instance.FullyQualifiedName, instance.Stage.Name, null);
			instance.SetMetrics(instance.Metrics with { NextAutoUtc = time.GetUtcNow() + TimeSpan.FromSeconds(1) });
			scanner.Wake();
			return;
		}
		instance.State = InstanceLifecycleState.Running;
		instance.SetMetrics(instance.Metrics with { NextAutoUtc = null });
		Log.BeginIteration(logger, instance.FullyQualifiedName, trigger, null);
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		// StageRunner.RunIterationAsync обязан вызвать concurrency.Release(stage) в finally.
		_ = Task.Run(async () => await runner.RunIterationAsync(instance, trigger, ct).ConfigureAwait(false), ct);
	}

	private void HandleKeyAdded(StageInstance source, string key) {
		if (source.State == InstanceLifecycleState.Terminating) {
			Log.IgnoredAddKeyFromTerminating(logger, source.Stage.Name, key, source.FullyQualifiedName, null);
			return;
		}
		if (!keyspace.Add(source.Stage.Name, source.DependencyKeys, key)) {
			Log.AddKeyAlreadyPresent(logger, source.Stage.Name, key, null);
			return;
		}
		Log.AddKeyAdded(logger, source.Stage.Name, key, null);
		foreach (var dependent in registry.StagesDependingOnInstance(source.Stage.Name)) {
			CreateAndStart(dependent);
		}
		scanner.Wake();
	}

	private async Task HandleKeyRemovedAsync(StageInstance source, string key, CancellationToken ct) {
		if (source.State == InstanceLifecycleState.Terminating) {
			Log.IgnoredRemoveKeyFromTerminating(logger, source.Stage.Name, key, source.FullyQualifiedName, null);
			return;
		}
		if (!keyspace.Remove(source.Stage.Name, source.DependencyKeys, key)) {
			Log.RemoveKeyAbsent(logger, source.Stage.Name, key, null);
			return;
		}
		await CascadeKeyRemovalAsync(source.Stage.Name, source.DependencyKeys, key, ct).ConfigureAwait(false);
		scanner.Wake();
	}

	/// <summary>
	/// <b>Итеративный</b> cascade через BFS-очередь: каждый шаг очереди — это (эмитер-стадия, его ключи, удалённый ключ).
	/// Для каждого аффектированного инстанса:
	/// <list type="bullet">
	/// <item>устанавливаем State=Terminating;</item>
	/// <item>если был Running — cancel <see cref="StageInstance.RunCts"/>, finalize отложен до StageCompleted/Failed;</item>
	/// <item>если был Idle — сразу finalize (Remove + RemoveScopeAsync);</item>
	/// <item>сирот-ключи из его keyspace-bucket → enqueue для дальнейшего обхода.</item>
	/// </list>
	/// Преимущество vs рекурсия: глубокие графы не порождают цепочки async-state-machine-frame'ов в куче.
	/// </summary>
	private async Task CascadeKeyRemovalAsync(
		string emitterStage,
		IReadOnlyDictionary<string, string> emitterKeys,
		string key,
		CancellationToken ct
	) {
		var queue = new Queue<(string Stage, IReadOnlyDictionary<string, string> EmitterKeys, string Key)>();
		queue.Enqueue((emitterStage, emitterKeys, key));

		while (queue.Count > 0) {
			var (es, ek, k) = queue.Dequeue();
			var affectedStages = registry.StagesAffectedByKeyRemoval(es);
			var affected = affectedStages
				.SelectMany(s => instances.InstancesOf(s.Name))
				.Where(inst => inst.State != InstanceLifecycleState.Terminating)
				.Where(inst => MatchesEmitter(inst.DependencyKeys, es, ek, k))
				.ToList();
			if (affected.Count == 0) continue;

			// Сортировка по pre-computed cancellation rank (листья — меньший ранг — отменяются первыми).
			affected.Sort((a, b) => registry.CancellationRank(a.Stage.Name).CompareTo(registry.CancellationRank(b.Stage.Name)));
			Log.RemoveKeyCascade(logger, es, k, affected.Count, null);

			foreach (var instance in affected) {
				// Снимаем bucket этого инстанса — orphan-ключи enqueue'ём для дальнейшего обхода.
				var orphans = keyspace.RemoveInstance(instance.Stage.Name, instance.DependencyKeys);
				foreach (var orphanKey in orphans) {
					queue.Enqueue((instance.Stage.Name, instance.DependencyKeys, orphanKey));
				}

				// Атомарный snapshot State + RunCts ДО State=Terminating, чтобы понять Running vs Idle ветку.
				var wasRunning = instance.State == InstanceLifecycleState.Running;
				var cts = instance.RunCts;    // volatile read
				instance.State = InstanceLifecycleState.Terminating;

				if (wasRunning) {
					// Defer cleanup: runner отстрелит StageCompleted/Failed, handler увидит Terminating → finalize.
					try { cts?.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
					Log.CascadeCancelRunning(logger, instance.FullyQualifiedName, null);
				} else {
					// Идиотическое сразу-удаление: будущего event'а от runner-а не будет.
					Log.CascadeRemoveIdle(logger, instance.FullyQualifiedName, null);
					await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
				}
			}
		}
	}

	/// <summary>
	/// True, если <paramref name="dependencyKeys"/> кандидата содержит <c>{emitterStage: key}</c>
	/// И ВСЕ <paramref name="emitterKeys"/> эмитера (т.е. кандидат был порождён именно этой комбинацией
	/// emitter+key, а не другим инстансом той же стадии-эмитера).
	/// </summary>
	private static bool MatchesEmitter(
		IReadOnlyDictionary<string, string> dependencyKeys,
		string emitterStage,
		IReadOnlyDictionary<string, string> emitterKeys,
		string key
	) {
		if (!dependencyKeys.TryGetValue(emitterStage, out var v) || !string.Equals(v, key, StringComparison.Ordinal)) return false;
		foreach (var kv in emitterKeys) {
			if (!dependencyKeys.TryGetValue(kv.Key, out var dv) || !string.Equals(dv, kv.Value, StringComparison.Ordinal)) return false;
		}
		return true;
	}

	private async Task HandleStageCompletedAsync(StageInstance instance, DateTimeOffset at, CancellationToken ct) {
		// Terminating: finalize cleanup, метрики не трогаем (инстанс «мёртв»).
		if (instance.State == InstanceLifecycleState.Terminating) {
			Log.TerminatingCompletedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}
		// Idempotence: если инстанс уже не Running — не двигаемся (могло прийти двойное событие).
		if (instance.State != InstanceLifecycleState.Running) return;

		try {
			var current = instance.Metrics;
			bool wasFirstSuccess = !current.LastSuccess.HasValue;
			// NextAutoUtc вычисляется от `at` (фактическое завершение runner-а), не от now — корректное
			// расписание даже под backlog'ом event-loop'а.
			instance.SetMetrics(new JobMetrics(
				LastSuccess: at,                   // монотонно: не сбрасывается на последующих неуспехах
				LastAttempt: at,
				ConsecutiveFailures: 0,
				LastError: null,
				NextAutoUtc: at + instance.Stage.Interval));

			if (wasFirstSuccess) {
				Log.FirstSuccessCascade(logger, instance.FullyQualifiedName, null);
				foreach (var dependent in EnumerateDirectDependents(instance.Stage.Name)) {
					CreateAndStart(dependent);
				}
			}
		} finally {
			// Guarantee: State выходит из Running при ЛЮБОМ исходе обработки (включая exception из EnumerateDirectDependents).
			instance.State = InstanceLifecycleState.Idle;
			scanner.Wake();
		}
	}

	private async Task HandleStageFailedAsync(StageInstance instance, Exception ex, DateTimeOffset at, CancellationToken ct) {
		// Terminating: finalize cleanup.
		if (instance.State == InstanceLifecycleState.Terminating) {
			Log.TerminatingFailedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}
		if (instance.State != InstanceLifecycleState.Running) return;

		try {
			var current = instance.Metrics;
			var failures = current.ConsecutiveFailures + 1;
			var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(failures);
			var nextDelay = retryDelay > TimeSpan.Zero ? retryDelay : instance.Stage.Interval;
			// LastSuccess НЕ меняется — монотонная метка. NextAutoUtc от `at`, не от now.
			instance.SetMetrics(current with {
				LastAttempt = at,
				ConsecutiveFailures = failures,
				LastError = ex.Message,
				NextAutoUtc = at + nextDelay,
			});
		} finally {
			instance.State = InstanceLifecycleState.Idle;
			scanner.Wake();
		}
	}

	private async Task FinalizeTerminatingAsync(StageInstance instance, CancellationToken ct) {
		// Remove из InstanceManager БЕФОRE RemoveScopeAsync — следующие lookup'ы не найдут.
		instances.Remove(instance);
		try {
			await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.FinalizeFailed(logger, instance.FullyQualifiedName, ex);
		}
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
	/// EventId-ы 3xxx — диапазон EventLoop.
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

		public static readonly Action<ILogger, string, string, Exception?> ConcurrencyDeferred =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3023, nameof(ConcurrencyDeferred)),
				"Iteration {Instance} отложена: лимит ConcurrencyLimit стадии {StageName} выбран; re-schedule через 1s");

		public static readonly Action<ILogger, string, string, string, Exception?> IgnoredAddKeyFromTerminating =
			LoggerMessage.Define<string, string, string>(LogLevel.Debug, new EventId(3007, nameof(IgnoredAddKeyFromTerminating)),
				"Ignored AddKey({StageName},{Key}) from terminating instance {Instance}");

		public static readonly Action<ILogger, string, string, Exception?> AddKeyAlreadyPresent =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3009, nameof(AddKeyAlreadyPresent)),
				"AddKey({StageName},{Key}) — ключ уже в keyspace, no-op");

		public static readonly Action<ILogger, string, string, Exception?> AddKeyAdded =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3010, nameof(AddKeyAdded)),
				"AddKey({StageName},{Key}) — ключ добавлен; cascade зависимым стадиям");

		public static readonly Action<ILogger, string, string, string, Exception?> IgnoredRemoveKeyFromTerminating =
			LoggerMessage.Define<string, string, string>(LogLevel.Debug, new EventId(3011, nameof(IgnoredRemoveKeyFromTerminating)),
				"Ignored RemoveKey({StageName},{Key}) from terminating instance {Instance}");

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
