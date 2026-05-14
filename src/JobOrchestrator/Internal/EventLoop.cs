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
/// <b>Try/finally state consistency.</b> В StageCompleted/Failed handler-е <see cref="Instance.State"/>=Idle
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
	SuccessWaiters successWaiters,
	OutcomeWaiters outcomeWaiters,
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
			// Завершаем все pending WaitFor-таски с InvalidOperationException — caller-ы получат
			// чёткий сигнал «оркестратор остановлен», а не зависшие Task'и.
			var stopReason = new InvalidOperationException("Оркестратор остановлен; WaitFor-ожидания не могут быть резолвлены.");
			successWaiters.FailAll(stopReason);
			outcomeWaiters.FailAll(stopReason);
		}
		Log.Stopped(logger, null);
	}

	private void DrainPendingRequests() {
		var stopReason = new InvalidOperationException("Оркестратор остановлен; ManualTrigger не может быть обслужен.");
		while (channel.Reader.TryRead(out var evt)) {
			if (evt is ManualTriggerRequestedEvent mt) {
				// Точная семантика: InvalidOperationException (graceful shutdown), а не TriggerResult.Faulted
				// (что подразумевает crash). Caller увидит чёткий exception вместо безмолвного «Faulted».
				mt.Tcs.TrySetException(stopReason);
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

	private void HandleTimerTick(Instance instance, CancellationToken ct) {
		// pendingTick освобождается ВСЕГДА при обработке tick-события.
		instance.ReleasePendingTick();
		// Идемпотентность: инстанс мог быть уже Terminated/удалён.
		if (instances.Find(instance.Identity) != instance) return;
		if (instance.IsTerminating || instance.IsRunning) return;
		var now = time.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(instance, TriggerSource.Auto, now);
		if (decision == TriggerResult.Started) {
			BeginIteration(instance, TriggerSource.Auto, ct);
		}
	}

	private void HandleManualTrigger(ManualTriggerRequestedEvent evt, CancellationToken ct) {
		// O(1) lookup через pre-computed Identity — без повторного Encode на каждый trigger.
		var instance = instances.Find(evt.Identity);
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

	private void BeginIteration(Instance instance, TriggerSource trigger, CancellationToken ct) {
		// ConcurrencyLimit: если стадия уже на лимите — re-schedule инстанс через короткое окно
		// (1 sec), не меняя State (остаётся Idle). DueScanner подберёт его снова, когда лимит откроется.
		if (!concurrency.TryAcquire(instance.Stage.Name)) {
			Log.ConcurrencyDeferred(logger, instance.FullyQualifiedName, instance.Stage.Name, null);
			instance.SetMetrics(instance.Metrics with { NextAutoUtc = time.GetUtcNow() + TimeSpan.FromSeconds(1) });
			scanner.Wake();
			return;
		}
		// CAS-перевод в Running. Идемпотентно: если кто-то уже стартанул этот инстанс (race на dup
		// TimerTickedEvent) — TryBeginRunning вернёт false и мы отпустим semaphore.
		if (!instance.TryBeginRunning()) {
			concurrency.Release(instance.Stage.Name);
			return;
		}
		instance.SetMetrics(instance.Metrics with { NextAutoUtc = null });
		Log.BeginIteration(logger, instance.FullyQualifiedName, trigger, null);
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		// StageRunner.RunIterationAsync обязан вызвать concurrency.Release + instance.EndRunning в finally.
		_ = Task.Run(async () => await runner.RunIterationAsync(instance, trigger, ct).ConfigureAwait(false), ct);
	}

	private void HandleKeyAdded(Instance source, string key) {
		if (source.IsTerminating) {
			Log.IgnoredAddKeyFromTerminating(logger, source.Stage.Name, key, source.FullyQualifiedName, null);
			return;
		}
		if (!keyspace.Add(source.Identity, key)) {
			Log.AddKeyAlreadyPresent(logger, source.Stage.Name, key, null);
			return;
		}
		Log.AddKeyAdded(logger, source.Stage.Name, key, null);
		foreach (var dependent in source.Stage.DependentsInstance) {
			CreateAndStart(dependent);
		}
		scanner.Wake();
	}

	private async Task HandleKeyRemovedAsync(Instance source, string key, CancellationToken ct) {
		if (source.IsTerminating) {
			Log.IgnoredRemoveKeyFromTerminating(logger, source.Stage.Name, key, source.FullyQualifiedName, null);
			return;
		}
		if (!keyspace.Remove(source.Identity, key)) {
			Log.RemoveKeyAbsent(logger, source.Stage.Name, key, null);
			return;
		}
		await CascadeKeyRemovalAsync(source.Identity, key, ct).ConfigureAwait(false);
		scanner.Wake();
	}

	/// <summary>
	/// <b>Итеративный</b> cascade через BFS-очередь: каждый шаг очереди — это (эмитер-стадия, его ключи, удалённый ключ).
	/// Для каждого аффектированного инстанса:
	/// <list type="bullet">
	/// <item>устанавливаем State=Terminating;</item>
	/// <item>если был Running — cancel <see cref="Instance.RunCts"/>, finalize отложен до StageCompleted/Failed;</item>
	/// <item>если был Idle — сразу finalize (Remove + RemoveScopeAsync);</item>
	/// <item>сирот-ключи из его keyspace-bucket → enqueue для дальнейшего обхода.</item>
	/// </list>
	/// Преимущество vs рекурсия: глубокие графы не порождают цепочки async-state-machine-frame'ов в куче.
	/// </summary>
	/// <summary>
	/// Сид BFS-обхода каскада: какой эмитер и какой его ключ инициировали удаление.
	/// Identity иммутабельна — валидна даже после удаления инстанса из <c>InstanceManager</c>.
	/// </summary>
	private readonly record struct CascadeSeed(InstanceIdentity Emitter, string Key);

	private async Task CascadeKeyRemovalAsync(InstanceIdentity emitter, string key, CancellationToken ct) {
		var queue = new Queue<CascadeSeed>();
		queue.Enqueue(new CascadeSeed(emitter, key));

		while (queue.Count > 0) {
			var seed = queue.Dequeue();
			var affected = seed.Emitter.Stage.AffectedByKeyRemoval
				.SelectMany(s => instances.InstancesOf(s.Name))
				.Where(inst => !inst.IsTerminating)
				.Where(inst => MatchesEmitter(inst, seed.Emitter, seed.Key))
				.ToList();
			if (affected.Count == 0) continue;

			// Сортировка по pre-computed cancellation rank (листья — меньший ранг — отменяются первыми).
			affected.Sort((a, b) => a.Stage.CancellationRank.CompareTo(b.Stage.CancellationRank));
			Log.RemoveKeyCascade(logger, seed.Emitter.Stage.Name, seed.Key, affected.Count, null);

			foreach (var instance in affected) {
				// CAS-перевод в Terminating. Если кто-то уже отметил (через другой orphan-ключ
				// в той же cascade-сессии) — пропускаем. MarkTerminating возвращает true ровно один раз.
				if (!instance.MarkTerminating()) continue;

				// Снимаем bucket этого инстанса — orphan-ключи enqueue'ём для дальнейшего обхода.
				// Identity иммутабельна, сохраняется в seed после Remove — для рекурсивного matching.
				var orphans = keyspace.RemoveInstance(instance.Identity);
				foreach (var orphanKey in orphans) {
					queue.Enqueue(new CascadeSeed(instance.Identity, orphanKey));
				}

				// Если runner всё ещё бежит — defer cleanup до StageCompleted/Failed handler-а.
				var cts = instance.RunCts;
				if (instance.IsRunning && cts is not null) {
					try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
					Log.CascadeCancelRunning(logger, instance.FullyQualifiedName, null);
				} else {
					Log.CascadeRemoveIdle(logger, instance.FullyQualifiedName, null);
					await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
				}
			}
		}
	}

	/// <summary>
	/// True, если <paramref name="candidate"/>-инстанс был порождён ИМЕННО ЭТОЙ комбинацией
	/// <paramref name="emitter"/> + <paramref name="key"/>: его <c>DependencyKeys</c> содержит
	/// <c>{emitter.Stage.Name: key}</c> и ВСЕ <c>emitter.DependencyKeys</c> с теми же значениями.
	/// </summary>
	private static bool MatchesEmitter(Instance candidate, InstanceIdentity emitter, string key) {
		var depKeys = candidate.DependencyKeys;
		if (!depKeys.TryGetValue(emitter.Stage.Name, out var v) || !string.Equals(v, key, StringComparison.Ordinal)) return false;
		foreach (var kv in emitter.DependencyKeys) {
			if (!depKeys.TryGetValue(kv.Key, out var dv) || !string.Equals(dv, kv.Value, StringComparison.Ordinal)) return false;
		}
		return true;
	}

	private async Task HandleStageCompletedAsync(Instance instance, DateTimeOffset at, CancellationToken ct) {
		// Terminating: finalize cleanup, метрики не трогаем (инстанс «мёртв»).
		if (instance.IsTerminating) {
			Log.TerminatingCompletedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}
		// Late event: runner отстрелил StageCompleted уже после того, как instance был удалён из
		// InstanceManager (теоретически возможно при race shutdown vs runner-finally). Лог + ignore.
		if (instances.Find(instance.Identity) != instance) {
			Log.LateStageEventForRemovedInstance(logger, nameof(StageCompletedEvent), instance.FullyQualifiedName, null);
			return;
		}
		// Idempotence: если runner не был активен — late event (already-processed).
		if (!instance.IsRunning) return;

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

			// Сигналим waiters ПОСЛЕ обновления метрик — late-register увидит LastSuccess через fast-path.
			successWaiters.SignalSuccess(instance.Stage.Name, instance.EncodedKey);
			outcomeWaiters.Signal(instance.Stage.Name, instance.EncodedKey, StageOutcome.Success);

			if (wasFirstSuccess) {
				Log.FirstSuccessCascade(logger, instance.FullyQualifiedName, null);
				foreach (var dependent in EnumerateDirectDependents(instance.Stage)) {
					CreateAndStart(dependent);
				}
			}
		} finally {
			// Guarantee: выход из Running при ЛЮБОМ исходе обработки. EndRunning одновременно
			// чистит pendingTick — гарантирует, что DueScanner может опубликовать следующий тик.
			instance.EndRunning();
			scanner.Wake();
		}
	}

	private async Task HandleStageFailedAsync(Instance instance, Exception ex, DateTimeOffset at, CancellationToken ct) {
		// Terminating: finalize cleanup.
		if (instance.IsTerminating) {
			Log.TerminatingFailedFinalize(logger, instance.FullyQualifiedName, null);
			await FinalizeTerminatingAsync(instance, ct).ConfigureAwait(false);
			return;
		}
		if (instances.Find(instance.Identity) != instance) {
			Log.LateStageEventForRemovedInstance(logger, nameof(StageFailedEvent), instance.FullyQualifiedName, null);
			return;
		}
		if (!instance.IsRunning) return;

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

			// Outcome=Failure. SuccessWaiters не сигналим — этот цикл не success.
			outcomeWaiters.Signal(instance.Stage.Name, instance.EncodedKey, StageOutcome.FromFailure(ex));
		} finally {
			instance.EndRunning();
			scanner.Wake();
		}
	}

	private async Task FinalizeTerminatingAsync(Instance instance, CancellationToken ct) {
		// Remove из InstanceManager БЕФОRE RemoveScopeAsync — следующие lookup'ы не найдут.
		instances.Remove(instance);

		// Уведомляем waiters об отмене: success-ожидание получает InvalidOperationException,
		// outcome-ожидание получает StageOutcomeKind.Cancelled. После этого Reset bucket-ы —
		// новый инстанс с теми же ключами начнёт с чистого состояния.
		var cancelReason = new InvalidOperationException($"Инстанс {instance.FullyQualifiedName} удалён каскадом.");
		successWaiters.SignalCancellation(instance.Stage.Name, instance.EncodedKey, cancelReason);
		outcomeWaiters.Signal(instance.Stage.Name, instance.EncodedKey, StageOutcome.FromCancellation(cancelReason));
		outcomeWaiters.Reset(instance.Stage.Name, instance.EncodedKey);

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

	/// <summary>
	/// Прямые dependent-стадии (whole ∪ instance) без дубликатов. Инвариант гарантируется
	/// <c>ConfigurationValidator.ValidateNoDuplicateDependencies</c> на build-time:
	/// <see cref="StageDescriptor.DependentsWhole"/> ∩ <see cref="StageDescriptor.DependentsInstance"/> = ∅
	/// и каждый список duplicate-free сам по себе. Defensive <c>DistinctBy</c> здесь не нужен.
	/// </summary>
	private static IEnumerable<StageDescriptor> EnumerateDirectDependents(StageDescriptor stage) =>
		stage.DependentsWhole.Concat(stage.DependentsInstance);

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

		public static readonly Action<ILogger, string, string, Exception?> LateStageEventForRemovedInstance =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(3024, nameof(LateStageEventForRemovedInstance)),
				"{EventType} прибыл для уже удалённого инстанса {Instance} — игнорируем (event-loop drained late event без потери)");
	}
}
