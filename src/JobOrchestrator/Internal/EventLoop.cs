using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using JobOrchestrator.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
[SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize",
	Justification = "Sealed class без финализатора — GC.SuppressFinalize был бы no-op и misleading.")]
internal sealed partial class EventLoop : IDisposable {
	private readonly StageRegistry registry;
	private readonly InstanceManager instances;
	private readonly DueScanner scanner;
	private readonly Channel<OrchestratorEvent> channel;
	private readonly IJobStateStore stateStore;
	private readonly IServiceProvider rootProvider;
	private readonly JobOrchestratorHostOptions hostOptions;
	private readonly JobOrchestratorRuntime runtime;
	private readonly ILogger<EventLoop> logger;
	private readonly ILogger _stageLogger;
	private readonly TimeProvider time;
	private readonly FrozenDictionary<StageDescriptor, SemaphoreSlim> _stageSemaphores;
	private readonly SemaphoreSlim? _globalSem;
	private int _handlerCrashCount;

	public EventLoop(
		StageRegistry registry,
		InstanceManager instances,
		DueScanner scanner,
		Channel<OrchestratorEvent> channel,
		IJobStateStore stateStore,
		IServiceProvider rootProvider,
		JobOrchestratorHostOptions hostOptions,
		JobOrchestratorRuntime runtime,
		ILogger<EventLoop> logger,
		ILoggerFactory loggerFactory,
		TimeProvider time
	) {
		this.registry = registry;
		this.instances = instances;
		this.scanner = scanner;
		this.channel = channel;
		this.stateStore = stateStore;
		this.rootProvider = rootProvider;
		this.hostOptions = hostOptions;
		this.runtime = runtime;
		this.logger = logger;
		_stageLogger = loggerFactory.CreateLogger("JobOrchestrator.StageRunner");
		this.time = time;

		// Per-stage семафоры: стадии без ConcurrencyLimit отсутствуют в словаре (TryAcquire = true бесплатно).
		var seed = new Dictionary<StageDescriptor, SemaphoreSlim>();
		foreach (var stage in registry.AllStages) {
			if (stage.ConcurrencyLimit is { } limit) {
				seed[stage] = new SemaphoreSlim(limit, limit);
			}
		}
		_stageSemaphores = seed.ToFrozenDictionary();

		// Глобальный лимит итераций поверх per-stage.
		if (registry.GlobalConcurrencyLimit is { } globalLimit) {
			if (globalLimit < 1) {
				throw new ArgumentOutOfRangeException(nameof(registry), globalLimit,
					"GlobalConcurrencyLimit должен быть >= 1.");
			}
			_globalSem = new SemaphoreSlim(globalLimit, globalLimit);
		}
	}

	/// <summary>
	/// <c>true</c>, если у стадии нет лимита или токен успешно захвачен — caller'у нужно отложить запуск
	/// при <c>false</c>. Вызывается только из event-loop-consumer-потока.
	/// </summary>
	private bool TryAcquireStageConcurrency(StageDescriptor stage) =>
		!_stageSemaphores.TryGetValue(stage, out var sem) || sem.Wait(0);

	private void ReleaseStageConcurrency(StageDescriptor stage) {
		if (_stageSemaphores.TryGetValue(stage, out var sem)) sem.Release();
	}

	private bool TryAcquireGlobalConcurrency() => _globalSem is null || _globalSem.Wait(0);

	private void ReleaseGlobalConcurrency() {
		if (_globalSem is not null) _globalSem.Release();
	}

	/// <summary>
	/// Освобождает оба лимита (per-stage + global). Используется как callback для
	/// <see cref="StageRunner.RunIterationAsync"/>, чтобы runner мог отпустить ресурсы в finally без
	/// собственной ссылки на EventLoop-инфраструктуру.
	/// </summary>
	internal void ReleaseConcurrencyFor(StageDescriptor stage) {
		ReleaseGlobalConcurrency();
		ReleaseStageConcurrency(stage);
	}

	/// <summary>
	/// Чистая функция принятия триггера (Auto или Manual). Применяет retry-delay и debounce-окно по правилам:
	/// <list type="bullet">
	/// <item>Running → <see cref="TriggerResult.AlreadyRunning"/> для любого источника.</item>
	/// <item>Terminating → <see cref="TriggerResult.Terminating"/> (инстанс в каскадном удалении).</item>
	/// <item>Auto в retry-delay (после неуспеха) → <see cref="TriggerResult.WaitingRetry"/>.</item>
	/// <item>Manual в окне debounce от <c>LastAttempt</c> → <see cref="TriggerResult.Debounced"/>.</item>
	/// <item>Manual игнорирует retry-delay (пользователь явно просит).</item>
	/// <item>Auto не проверяет debounce (scheduled tick через interval — анти-spam-click не нужен).</item>
	/// </list>
	/// </summary>
	internal static TriggerAcceptanceDecision TryAcceptTrigger(Instance instance, TriggerSource source, DateTimeOffset now) {
		var state = instance.State;
		if (state == InstanceLifecycleState.Terminating) return TriggerAcceptanceDecision.Reject(TriggerResult.Terminating);
		if (state == InstanceLifecycleState.Running) return TriggerAcceptanceDecision.Reject(TriggerResult.AlreadyRunning);

		var stats = instance.Metrics.Stats;
		if (source == TriggerSource.Auto) {
			if (stats.ConsecutiveFailures > 0 && stats.LastAttempt.HasValue) {
				var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(stats.ConsecutiveFailures);
				if (now - stats.LastAttempt.Value < retryDelay) {
					return TriggerAcceptanceDecision.Reject(TriggerResult.WaitingRetry);
				}
			}
			return TriggerAcceptanceDecision.Accept;
		}
		if (stats.LastAttempt.HasValue && (now - stats.LastAttempt.Value) < instance.Stage.Debounce) {
			return TriggerAcceptanceDecision.Reject(TriggerResult.Debounced);
		}
		return TriggerAcceptanceDecision.Accept;
	}

	public void Dispose() {
		_globalSem?.Dispose();
		foreach (var sem in _stageSemaphores.Values) sem.Dispose();
	}

	public async Task RunAsync(CancellationToken stoppingToken) {
		Log.Starting(logger, registry.AllStages.Count, null);
		BootstrapInitialInstances();
		// После bootstrap-а у DueScanner появляется работа — будим его.
		scanner.Wake();
		try {
			await foreach (var evt in channel.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false)) {
				try {
					await HandleEventAsync(evt, stoppingToken).ConfigureAwait(false);
					_handlerCrashCount = 0;
				} catch (Exception ex) {
					_handlerCrashCount++;
					Log.HandlerCrashed(logger, evt.GetType().Name, ex);
					if (evt is ManualTriggerRequestedEvent mt) {
						// Заворачиваем причину в IterationRejectedException — caller'у обещано, что reject
						// приходит ИМЕННО этим типом. Inner = оригинальный exception handler'а, stack-trace
						// сохраняется для диагностики.
						var rejected = new IterationRejectedException(
							IterationRejectReason.Faulted,
							mt.Identity.FullyQualifiedName,
							$"Сбой обработчика события {evt.GetType().Name}; запуск {mt.Identity.FullyQualifiedName} отвергнут.",
							ex);
						mt.Tcs.TrySetException(rejected);
					}
					var stateMutatingCrash = hostOptions.FaultOnStateMutatingHandlerCrash && IsStateMutatingHandlerEvent(evt);
					if (stateMutatingCrash) {
						Log.StateMutatingHandlerCrashFault(logger, evt.GetType().Name, null);
					}
					if (stateMutatingCrash || _handlerCrashCount >= hostOptions.HandlerCrashFaultThreshold) {
						if (!stateMutatingCrash) {
							Log.RepeatedHandlerCrashes(logger, _handlerCrashCount, null);
						}
						runtime.MarkFaulted();
						break;
					}
				}
			}
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			// Нормальный shutdown.
		} catch (ChannelClosedException) {
			// Канал закрыт извне (lifecycle.MarkFaulted/CloseChannel). Нормальный shutdown.
		} finally {
			// Закрываем channel ПЕРВЫМ делом: между exit'ом из ReadAllAsync и финализацией ниже
			// HostedService.ExecuteAsync ещё не успел вызвать свой CloseChannel — race-окно, в которое
			// внешние Runtime.RunAsync/RegisterKey могут проскочить WriteAsync (channel ещё open) и
			// зависнуть на await tcs.Task, потому что consumer уже ушёл. TryComplete идемпотентен.
			runtime.CloseChannel();
			DrainPendingRequests();
			// Финализируем все Terminating-инстансы (их finalize-events не прилетят при закрытом Channel).
			await FinalizeAllTerminatingAsync().ConfigureAwait(false);
			// WaitForSuccessAsync теперь реализован через broadcaster'ы: завершение subscriber-каналов
			// (Changes + iteration stream) выполняется в JobOrchestratorRuntime.OnShutdown, что делает
			// runtime'ом HostedService после нашего exit. Поэтому здесь — только iteration-outcome TCS:
			// runner мог не успеть опубликовать completion-event перед закрытием channel.
			var stopReason = new InvalidOperationException("Оркестратор остановлен; ожидания не могут быть резолвлены.");
			foreach (var inst in instances.All) {
				inst.TakeIterationOutcomeTcs()?.TrySetException(
					IterationFailures.OrchestratorShutdown(inst, stopReason));
			}
		}
		Log.Stopped(logger, null);
	}

	/// <summary>
	/// Дочищает оставшиеся в Channel события после остановки event-loop (по <c>stoppingToken</c> либо
	/// <see cref="ChannelClosedException"/>). Вызывается ДО <c>FailAll</c>-waiter-ов — это единственный
	/// шанс отдать реальный исход подписчикам, которые иначе получат «оркестратор остановлен» вместо
	/// фактического Success/Failure.
	/// <para>
	/// Для completion-событий — метрики + waiters + <c>EndRunning</c> (как в handler, без first-success cascade).
	/// Cascade в закрытый channel не ставим.
	/// </para>
	/// </summary>
	private void DrainPendingRequests() {
		var stopReason = new InvalidOperationException("Оркестратор остановлен; WaitFor-ожидания не могут быть резолвлены.");
		while (channel.Reader.TryRead(out var evt)) {
			switch (evt) {
				case ManualTriggerRequestedEvent mt:
					// Контракт RunAsync: reject — всегда IterationRejectedException. Drain в shutdown
					// относится к pending request'у, который ещё не получил handle — это reject.
					mt.Tcs.TrySetException(new IterationRejectedException(
						IterationRejectReason.Faulted,
						mt.Identity.FullyQualifiedName,
						$"Оркестратор остановлен; запуск {mt.Identity.FullyQualifiedName} не может быть обслужен."));
					break;
				// StageCompleted/Failed могут оказаться в Channel если runner опубликовал событие
				// уже после того, как ReadAllAsync завершился (stoppingToken). EndRunning снимает
				// _running-флаг + сигналим waiter-ам реальный исход (иначе FailAll ниже резолвит их
				// «остановлен», маскируя фактический Success/Failure).
				case StageCompletedEvent sc:
					DrainStageCompleted(sc);
					break;
				case StageFailedEvent sf:
					DrainStageFailed(sf);
					break;
				case TimerTickedEvent tt:
					tt.Instance.ReleasePendingTick();
					break;
				case KeyAddedEvent ka:
					Log.DrainDiscardedEvent(logger, nameof(KeyAddedEvent), ka.Source.FullyQualifiedName, ka.Key, null);
					break;
				case KeyRemovedEvent kr:
					Log.DrainDiscardedEvent(logger, nameof(KeyRemovedEvent), kr.Source.FullyQualifiedName, kr.Key, null);
					break;
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
			if (instances.Remove(instance)) runtime.NotifyInstanceRemoved(instance);
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
		var decision = TryAcceptTrigger(instance, TriggerSource.Auto, now);
		if (decision.IsAccepted) {
			BeginIteration(instance, TriggerSource.Auto, ct);
		}
	}

	private void HandleManualTrigger(ManualTriggerRequestedEvent evt, CancellationToken ct) {
		// Fast-path: caller отменил свой CancellationToken до того, как мы дошли до обработки.
		// Runtime через ct.UnsafeRegister отменяет evt.Tcs синхронно при cancellation → IsCompleted=true.
		// Не запускаем итерацию — caller уже ушёл, side-effects бесполезны.
		if (evt.Tcs.Task.IsCompleted) {
			Log.ManualTriggerCallerCancelled(logger, evt.Identity.FullyQualifiedName, null);
			return;
		}
		// O(1) lookup через pre-computed Identity — без повторного Encode на каждый trigger.
		var instance = instances.Find(evt.Identity);
		if (instance is null) {
			Reject(evt, IterationRejectReason.NotFound, evt.Identity.FullyQualifiedName);
			return;
		}
		var now = time.GetUtcNow();
		var decision = TryAcceptTrigger(instance, TriggerSource.Manual, now);
		if (!decision.IsAccepted) {
			Reject(evt, MapTriggerResultToReason(decision.Rejection!.Value), instance.FullyQualifiedName);
			return;
		}
		var (result, handle) = BeginIteration(instance, TriggerSource.Manual, ct);
		if (result != TriggerResult.Started) {
			Reject(evt, MapTriggerResultToReason(result), instance.FullyQualifiedName);
			return;
		}
		// Started → BeginIteration уже привязал iteration-scoped TCS + handle к Instance через
		// Instance.OnIterationStarted. Caller получает тот же handle, что и подписчики broadcaster'а.
		evt.Tcs.TrySetResult(handle!);
	}

	private void Reject(ManualTriggerRequestedEvent evt, IterationRejectReason reason, string fqn) {
		Log.IterationRejected(logger, fqn, reason, null);
		evt.Tcs.TrySetException(new IterationRejectedException(
			reason, fqn, $"Запуск инстанса {fqn} отвергнут: {reason}."));
	}

	private static IterationRejectReason MapTriggerResultToReason(TriggerResult result) => result switch {
		TriggerResult.NotFound => IterationRejectReason.NotFound,
		TriggerResult.Terminating => IterationRejectReason.Terminating,
		TriggerResult.AlreadyRunning => IterationRejectReason.AlreadyRunning,
		TriggerResult.Debounced => IterationRejectReason.Debounced,
		TriggerResult.ConcurrencyDeferred => IterationRejectReason.ConcurrencyDeferred,
		TriggerResult.Faulted => IterationRejectReason.Faulted,
		// Manual не должен получать Started здесь (caller обрабатывает Started отдельно) и WaitingRetry
		// (TriggerAcceptance.TryAccept не возвращает WaitingRetry для Manual). Любое попадание сюда —
		// нарушение инварианта event-loop'а, fail-fast.
		TriggerResult.Started => throw new InvalidOperationException(
			"Invariant violation: TriggerResult.Started не должен маппиться через MapTriggerResultToReason."),
		TriggerResult.WaitingRetry => throw new InvalidOperationException(
			"Invariant violation: Manual не должен получать TriggerResult.WaitingRetry от TriggerAcceptance."),
		_ => throw new InvalidOperationException($"Unknown TriggerResult: {result}"),
	};

	/// <summary>
	/// Запускает итерацию инстанса. Возвращает фактический результат и (при Started) handle итерации.
	/// <list type="bullet">
	/// <item><see cref="TriggerResult.Started"/> — итерация запущена на ThreadPool; handle привязан к
	/// <see cref="Instance"/> через outcome-TCS и iteration-broadcaster.</item>
	/// <item><see cref="TriggerResult.ConcurrencyDeferred"/> — лимит, handle null. Для Auto-тика
	/// re-schedule через ~1 сек + jitter; для Manual — возврат caller'у без re-schedule.</item>
	/// </list>
	/// <see cref="HandleManualTrigger"/> возвращает caller'у тот же handle, который попадает в
	/// per-instance iteration-broadcaster.
	/// </summary>
	private (TriggerResult Result, IIterationHandle? Handle) BeginIteration(Instance instance, TriggerSource trigger, CancellationToken ct) {
		// Сначала per-stage — он чаще узкий (типично 1-2). Только после успеха per-stage захватываем
		// глобальный, чтобы не делать холостые global-acquire/release-циклы под нагрузкой
		// «много стадий с ConcurrencyLimit=1 + длинные runner-ы».
		if (!TryAcquireStageConcurrency(instance.Stage)) {
			DeferIteration(instance, trigger);
			return (TriggerResult.ConcurrencyDeferred, null);
		}
		if (!TryAcquireGlobalConcurrency()) {
			ReleaseStageConcurrency(instance.Stage);
			DeferIteration(instance, trigger);
			return (TriggerResult.ConcurrencyDeferred, null);
		}
		if (!instance.TryBeginRunning()) {
			ReleaseConcurrencyFor(instance.Stage);
			throw new InvalidOperationException(
				$"Invariant violation: инстанс {instance.FullyQualifiedName} уже Running до BeginIteration. " +
				"Event-loop single-threaded contract нарушен.");
		}
		instance.SetMetrics(instance.Metrics.WithNextAutoUtc(null));
		try {
			// Iteration-scoped TCS + handle создаются ВСЕГДА (Manual и Auto). Manual: caller получает handle
			// возвратом BeginIteration. Auto: handle публикуется только в broadcaster и в Instance.RunningIteration.
			var outcomeTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
			var handle = new IterationHandle(runtime, instance.Identity, outcomeTcs.Task);
			instance.OnIterationStarted(handle, outcomeTcs);
			Log.BeginIteration(logger, instance.FullyQualifiedName, trigger, null);
			var stageRef = instance.Stage;
			Action releaseConcurrency = () => ReleaseConcurrencyFor(stageRef);
			_ = Task.Run(() => RunIterationSafeAsync(instance, trigger, releaseConcurrency, ct), ct);
			return (TriggerResult.Started, handle);
		} catch {
			// EndRunning() clear-ит _runningIteration + _running + _pendingTick одним вызовом.
			// TakeIterationOutcomeTcs() освобождает iteration-scoped TCS (caller-у его всё равно не доставили).
			instance.TakeIterationOutcomeTcs();
			instance.EndRunning();
			ReleaseConcurrencyFor(instance.Stage);
			throw;
		}
	}

	private async Task RunIterationSafeAsync(Instance instance, TriggerSource trigger, Action releaseConcurrency, CancellationToken ct) {
		try {
			await RunIterationAsync(instance, trigger, releaseConcurrency, runtime.WorkersCancellationToken, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.UnhandledIterationFault(logger, instance.FullyQualifiedName, ex);
		}
	}

	/// <summary>
	/// Запускает одну итерацию инстанса в свежем DI-scope с watchdog-CTS, формирует <see cref="JobContext"/>,
	/// разворачивает logger scope со структурными полями и публикует <see cref="StageCompletedEvent"/> или <see cref="StageFailedEvent"/>.
	/// <para>
	/// <paramref name="releaseConcurrency"/> вызывается в finally — освобождает per-stage + global семафоры.
	/// <paramref name="workersToken"/> — токен, cancel-ящийся при crash event loop (через <see cref="JobOrchestratorRuntime.MarkFaulted"/>);
	/// сшивается с shutdown/watchdog/cascade в linked CTS.
	/// </para>
	/// </summary>
	private async Task RunIterationAsync(Instance instance, TriggerSource trigger, Action releaseConcurrency, CancellationToken workersToken, CancellationToken stoppingToken) {
		string correlationId = Guid.NewGuid().ToString("N");
		var logFields = BuildLogScopeFields(instance, correlationId);

		await using var scope = rootProvider.CreateAsyncScope();
		using var loggerScope = _stageLogger.BeginScope(logFields);

		// Различаем источники cancel через ОТДЕЛЬНЫЕ CTS:
		// - stoppingToken — shutdown хоста;
		// - workersToken — crash event loop;
		// - watchdogCts — ExecutionTimeout превышен;
		// - cascadeCts (instance.RunCts) — событие-loop отменил из-за cascade-removal.
		// runCts — linked-источник всех вышеперечисленных, передаётся в IJobService.ExecuteAsync.
		var watchdogCts = instance.Stage.ExecutionTimeout is { } timeout
			? new CancellationTokenSource(timeout)
			: null;
		var cascadeCts = new CancellationTokenSource();
		var runCts = watchdogCts is not null
			? CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, workersToken, watchdogCts.Token, cascadeCts.Token)
			: CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, workersToken, cascadeCts.Token);
		// EventLoop вызывает Cancel() на cascadeCts (через instance.RunCts) для cascade-removal.
		// Не CAS: event-loop single-threaded, между EndRunning предыдущего runner-а и стартом этого
		// нет concurrent-writer'а к RunCts.
		instance.RunCts = cascadeCts;

		var completionPublished = false;
		try {
			var service = (IJobService)scope.ServiceProvider.GetRequiredService(instance.Stage.ServiceType);
			var jobState = new DefaultJobState(stateStore, instance.StateScope);

			var jobContext = new JobContext {
				CorrelationId = correlationId,
				Trigger = trigger,
				State = jobState,
				LastSuccessAt = instance.Metrics.Stats.LastSuccess,
				StageName = instance.Stage.Name,
				Keys = instance.Identity.Keys,
				FullyQualifiedName = instance.FullyQualifiedName,
				Sink = instance.Sink,
			};

			Log.IterationStart(_stageLogger, instance.FullyQualifiedName, trigger, null);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			Log.IterationCompleted(_stageLogger, instance.FullyQualifiedName, null);
			completionPublished = channel.Writer.Publish(new StageCompletedEvent(instance, time.GetUtcNow()));
			if (!completionPublished) {
				Log.CompletionNotPublished(_stageLogger, instance.FullyQualifiedName, nameof(StageCompletedEvent), null);
			}
		} catch (OperationCanceledException oce) {
			Exception failure;
			if (stoppingToken.IsCancellationRequested) {
				Log.IterationCancelledShutdown(_stageLogger, instance.FullyQualifiedName, null);
				failure = oce;
			} else if (watchdogCts?.IsCancellationRequested is true) {
				Log.IterationCancelledWatchdog(_stageLogger, instance.FullyQualifiedName, null);
				failure = new TimeoutException(
					$"Стадия {instance.FullyQualifiedName} превысила ExecutionTimeout ({instance.Stage.ExecutionTimeout}).",
					oce);
			} else if (cascadeCts.IsCancellationRequested) {
				Log.IterationCancelledCascade(_stageLogger, instance.FullyQualifiedName, null);
				failure = oce;
			} else {
				Log.IterationFailed(_stageLogger, instance.FullyQualifiedName, oce);
				failure = oce;
			}
			completionPublished = PublishStageFailed(instance, failure);
		} catch (Exception ex) {
			Log.IterationFailed(_stageLogger, instance.FullyQualifiedName, ex);
			completionPublished = PublishStageFailed(instance, ex);
		} finally {
			instance.ClearRunCtsIfEquals(cascadeCts);
			runCts.Dispose();
			watchdogCts?.Dispose();
			cascadeCts.Dispose();
			releaseConcurrency();
			if (!completionPublished) {
				instance.EndRunning();
			}
		}
	}

	private bool PublishStageFailed(Instance instance, Exception failure) {
		if (channel.Writer.Publish(new StageFailedEvent(instance, failure, time.GetUtcNow()))) {
			return true;
		}
		Log.CompletionNotPublished(_stageLogger, instance.FullyQualifiedName, nameof(StageFailedEvent), null);
		return false;
	}

	private static Dictionary<string, object> BuildLogScopeFields(Instance instance, string correlationId) {
		Dictionary<string, object> fields = new(3 + instance.DependencyKeys.Count, StringComparer.Ordinal) {
			["CorrelationId"] = correlationId,
			["FullyQualifiedName"] = instance.FullyQualifiedName,
			["StageName"] = instance.Stage.Name,
		};
		foreach (var kv in instance.DependencyKeys) {
			fields[$"{kv.Key}Key"] = kv.Value;
		}
		return fields;
	}

	private void DeferIteration(Instance instance, TriggerSource trigger) {
		Log.ConcurrencyDeferred(logger, instance.FullyQualifiedName, instance.Stage.Name, null);
		// Manual: caller сразу получает ConcurrencyDeferred и сам решает, когда повторить —
		// не трогаем NextAutoUtc (existing auto-расписание остаётся), не будим scanner.
		// Auto-tick: re-schedule через NextAutoUtc + jitter, DueScanner подберёт при освобождении лимита.
		if (trigger != TriggerSource.Auto) return;
		instance.SetMetrics(instance.Metrics.WithNextAutoUtc(ComputeDeferredNextAutoUtc()));
		scanner.Wake();
	}

	private DateTimeOffset ComputeDeferredNextAutoUtc() {
		var delay = TimeSpan.FromSeconds(1);
		var jitterMaxMs = hostOptions.ConcurrencyDeferJitterMaxMilliseconds;
		if (jitterMaxMs > 0) {
			delay += TimeSpan.FromMilliseconds(Random.Shared.Next(0, jitterMaxMs + 1));
		}
		return time.GetUtcNow() + delay;
	}

	/// <summary>
	/// Событие, обработка которого мутирует граф/keyspace/метрики оркестратора. Сбой handler'а на таком
	/// событии оставляет частичное состояние, поэтому при <see cref="JobOrchestratorHostOptions.FaultOnStateMutatingHandlerCrash"/>
	/// сразу триггерим fault.
	/// <para>
	/// <see cref="ManualTriggerRequestedEvent"/> сознательно НЕ включён: это внешний user-request,
	/// и его сбой не должен валить весь оркестратор — caller всё равно получает exception через
	/// <c>mt.Tcs.TrySetException(ex)</c> в catch-блоке выше. <see cref="TimerTickedEvent"/> включён,
	/// потому что внутренний tick → <c>BeginIteration</c> → семафоры/<c>RunCts</c>/<c>_running</c>
	/// могут оставить частично-захваченные ресурсы при сбое.
	/// </para>
	/// </summary>
	private static bool IsStateMutatingHandlerEvent(OrchestratorEvent evt) =>
		evt is KeyAddedEvent or KeyRemovedEvent or StageCompletedEvent or StageFailedEvent or TimerTickedEvent;

	private void DrainStageCompleted(StageCompletedEvent sc) {
		var instance = sc.Instance;
		if (instance.IsTerminating) {
			instance.TakeIterationOutcomeTcs()?.TrySetException(
				IterationFailures.CancelledByCascade(instance));
			instance.EndRunning();
			return;
		}
		if (instances.Find(instance.Identity) != instance) return;
		if (!instance.IsRunning) return;

		ApplyStageCompletedMetrics(instance, sc.At);
		instance.TakeIterationOutcomeTcs()?.TrySetResult();
		instance.EndRunning();
	}

	private void DrainStageFailed(StageFailedEvent sf) {
		var instance = sf.Instance;
		if (instance.IsTerminating) {
			instance.TakeIterationOutcomeTcs()?.TrySetException(
				IterationFailures.CancelledByCascade(instance));
			instance.EndRunning();
			return;
		}
		if (instances.Find(instance.Identity) != instance) return;
		if (!instance.IsRunning) return;

		ApplyStageFailedMetrics(instance, sf.Exception, sf.At);
		instance.TakeIterationOutcomeTcs()?.TrySetException(
			IterationFailures.StageHandlerFailed(instance, sf.Exception));
		instance.EndRunning();
	}

	private static bool ApplyStageCompletedMetrics(Instance instance, DateTimeOffset at) {
		var current = instance.Metrics;
		bool wasFirstSuccess = !current.Stats.LastSuccess.HasValue;
		instance.SetMetrics(new JobMetrics(
			Stats: new InstanceExecutionStats(
				LastSuccess: at,
				LastAttempt: at,
				ConsecutiveFailures: 0,
				LastError: null),
			Schedule: new InstanceSchedule(NextAutoUtc: at + instance.Stage.Interval)));
		return wasFirstSuccess;
	}

	private static void ApplyStageFailedMetrics(Instance instance, Exception ex, DateTimeOffset at) {
		var current = instance.Metrics;
		var failures = current.Stats.ConsecutiveFailures + 1;
		var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(failures);
		var nextDelay = retryDelay > TimeSpan.Zero ? retryDelay : instance.Stage.Interval;
		instance.SetMetrics(current with {
			Stats = current.Stats with {
				LastAttempt = at,
				ConsecutiveFailures = failures,
				LastError = ex.Message,
			},
			Schedule = current.Schedule with { NextAutoUtc = at + nextDelay },
		});
	}

	private void HandleKeyAdded(Instance source, string key) {
		if (source.IsTerminating) {
			Log.IgnoredAddKeyFromTerminating(logger, source.Stage.Name, key, source.FullyQualifiedName, null);
			return;
		}
		if (!source.AddEmittedKey(key)) {
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
		if (!source.RemoveEmittedKey(key)) {
			Log.RemoveKeyAbsent(logger, source.Stage.Name, key, null);
			return;
		}
		await CascadeKeyRemovalAsync(source.Identity, key, ct).ConfigureAwait(false);
		scanner.Wake();
	}

	/// <summary>
	/// Сид BFS-обхода каскада (см. <see cref="CascadeKeyRemovalAsync"/>): какой эмитер и какой его ключ
	/// инициировали удаление. <see cref="InstanceIdentity"/> иммутабельна — seed валиден даже после
	/// удаления инстанса из <see cref="InstanceManager"/>.
	/// </summary>
	private readonly record struct CascadeSeed(InstanceIdentity Emitter, string Key);

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

	private async Task CascadeKeyRemovalAsync(InstanceIdentity emitter, string key, CancellationToken ct) {
		var queue = new Queue<CascadeSeed>();
		queue.Enqueue(new CascadeSeed(emitter, key));

		while (queue.Count > 0) {
			var seed = queue.Dequeue();
			var affected = new List<Instance>();
			foreach (var s in seed.Emitter.Stage.AffectedByKeyRemoval) {
				foreach (var inst in instances.InstancesOf(s)) {
					if (inst.IsTerminating) continue;
					var depKeys = inst.DependencyKeys;
					if (!depKeys.TryGetValue(seed.Emitter.Stage.Name, out var v) || !string.Equals(v, seed.Key, StringComparison.Ordinal)) continue;
					bool emitterKeysMatch = true;
					foreach (var kv in seed.Emitter.DependencyKeys) {
						if (!depKeys.TryGetValue(kv.Key, out var dv) || !string.Equals(dv, kv.Value, StringComparison.Ordinal)) {
							emitterKeysMatch = false;
							break;
						}
					}
					if (emitterKeysMatch) affected.Add(inst);
				}
			}
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
				var orphans = instance.TakeEmittedKeys();
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
			// ApplyStageCompletedMetrics обновляет Snapshot.LastSuccess; этого достаточно для
			// WaitForSuccessAsync-extension'а — он читает LastSuccess в fast-path и в re-check'ах между
			// фазами своей реализации.
			bool wasFirstSuccess = ApplyStageCompletedMetrics(instance, at);
			// Iteration-scoped TCS (если RunAsync привязал его на BeginIteration) — резолвим success.
			instance.TakeIterationOutcomeTcs()?.TrySetResult();

			if (wasFirstSuccess) {
				Log.FirstSuccessCascade(logger, instance.FullyQualifiedName, null);
				// Прямые dependent-стадии: DependentsWhole ∪ DependentsInstance без дубликатов.
				// Инвариант гарантируется ConfigurationValidator.ValidateNoDuplicateDependencies на
				// build-time (списки не пересекаются и каждый сам duplicate-free).
				foreach (var dependent in instance.Stage.DependentsWhole) CreateAndStart(dependent);
				foreach (var dependent in instance.Stage.DependentsInstance) CreateAndStart(dependent);
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
			ApplyStageFailedMetrics(instance, ex, at);

			// Iteration-scoped TCS (RunAsync) — отстреливаем IterationFailedException со stage-исключением
			// в InnerException. WaitForSuccessAsync через iteration-broadcaster получит этот fail в
			// своём stream-loop'е (await iter.Completion бросит IterationFailedException) и продолжит
			// ждать следующую итерацию.
			instance.TakeIterationOutcomeTcs()?.TrySetException(
				IterationFailures.StageHandlerFailed(instance, ex));
		} finally {
			instance.EndRunning();
			scanner.Wake();
		}
	}

	private async Task FinalizeTerminatingAsync(Instance instance, CancellationToken ct) {
		// Remove из InstanceManager БЕФОRE RemoveScopeAsync — следующие lookup'ы не найдут.
		// TryRemove работает как однократный CAS-гард: при race между cascade-веткой (Idle → finalize
		// синхронно) и StageCompleted/Failed-handler-ом (тот же инстанс уже отстрелил completion-event)
		// один вызывающий получит true, второй — false и просто выйдет. Это страхует от повторной
		// сигнализации waiters и повторного RemoveScopeAsync.
		if (!instances.Remove(instance)) return;
		// Notify ПЕРЕД RemoveScopeAsync: iteration-broadcaster инстанса complete'ится сразу,
		// и waiter-ы выходят из await foreach без ожидания I/O state-store.
		runtime.NotifyInstanceRemoved(instance);

		// Iteration-scoped TCS (RunAsync) — отстреливаем IterationFailedException(Cancelled).
		// WaitForSuccessAsync-extension'у достаточно того, что iteration-broadcaster инстанса
		// completes через Runtime.NotifyInstanceRemoved → instance.CompleteIterationSubscribers,
		// и его stream loop завершится с throw InvalidOperationException.
		instance.TakeIterationOutcomeTcs()?.TrySetException(
			IterationFailures.CancelledByCascade(instance));

		try {
			await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.FinalizeFailed(logger, instance.FullyQualifiedName, ex);
		}
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = EvaluateAndCreate(stage, instances, channel, time);
		foreach (var instance in created) {
			runtime.NotifyInstanceAdded(instance);
			Log.InstanceCreated(logger, instance.FullyQualifiedName, null);
		}
	}

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

		public static readonly Action<ILogger, string, string, Exception?> ConcurrencyDeferred =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(3023, nameof(ConcurrencyDeferred)),
				"Iteration {Instance} отложена: лимит ConcurrencyLimit стадии {StageName} выбран; re-schedule через 1s");

		public static readonly Action<ILogger, string, string, Exception?> LateStageEventForRemovedInstance =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(3024, nameof(LateStageEventForRemovedInstance)),
				"{EventType} прибыл для уже удалённого инстанса {Instance} — игнорируем (event-loop drained late event без потери)");

		public static readonly Action<ILogger, int, Exception?> RepeatedHandlerCrashes =
			LoggerMessage.Define<int>(LogLevel.Warning, new EventId(3025, nameof(RepeatedHandlerCrashes)),
				"Event loop: {CrashCount} подряд сбоев обработчиков событий — оркестратор переведён в fault");

		public static readonly Action<ILogger, string, Exception?> UnhandledIterationFault =
			LoggerMessage.Define<string>(LogLevel.Error, new EventId(3026, nameof(UnhandledIterationFault)),
				"Необработанное исключение итерации {Instance} (runner завершился вне StageFailed)");

		public static readonly Action<ILogger, string, string, string, Exception?> DrainDiscardedEvent =
			LoggerMessage.Define<string, string, string>(LogLevel.Debug, new EventId(3027, nameof(DrainDiscardedEvent)),
				"Shutdown drain: событие {EventType} для {Instance} (key={Key}) снято без обработки");

		public static readonly Action<ILogger, string, Exception?> StateMutatingHandlerCrashFault =
			LoggerMessage.Define<string>(LogLevel.Critical, new EventId(3029, nameof(StateMutatingHandlerCrashFault)),
				"Сбой handler'а на мутирующем событии {EventType} — оркестратор переведён в fault (частичное состояние недопустимо)");

		public static readonly Action<ILogger, string, IterationRejectReason, Exception?> IterationRejected =
			LoggerMessage.Define<string, IterationRejectReason>(LogLevel.Debug, new EventId(3030, nameof(IterationRejected)),
				"Manual-запуск {Instance} отвергнут: {Reason}");

		public static readonly Action<ILogger, string, Exception?> ManualTriggerCallerCancelled =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(3031, nameof(ManualTriggerCallerCancelled)),
				"Manual-запуск {Instance} пропущен — caller отменил cancellation token до обработки события");

		// EventId-ы 4xxx — диапазон stage-iteration logs (бывший StageRunner).
		public static readonly Action<ILogger, string, TriggerSource, Exception?> IterationStart =
			LoggerMessage.Define<string, TriggerSource>(LogLevel.Debug, new EventId(4001, nameof(IterationStart)),
				"Старт итерации {Instance} (trigger={Trigger}).");

		public static readonly Action<ILogger, string, Exception?> IterationCompleted =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(4002, nameof(IterationCompleted)),
				"Итерация {Instance} успешно завершена.");

		public static readonly Action<ILogger, string, Exception?> IterationCancelledShutdown =
			LoggerMessage.Define<string>(LogLevel.Information, new EventId(4003, nameof(IterationCancelledShutdown)),
				"Итерация {Instance} отменена при shutdown.");

		public static readonly Action<ILogger, string, Exception?> IterationCancelledWatchdog =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(4004, nameof(IterationCancelledWatchdog)),
				"Итерация {Instance} превысила ExecutionTimeout (watchdog).");

		public static readonly Action<ILogger, string, Exception?> IterationFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(4005, nameof(IterationFailed)),
				"Итерация {Instance} завершилась с ошибкой.");

		public static readonly Action<ILogger, string, Exception?> IterationCancelledCascade =
			LoggerMessage.Define<string>(LogLevel.Information, new EventId(4006, nameof(IterationCancelledCascade)),
				"Итерация {Instance} отменена при cascade-removal.");

		public static readonly Action<ILogger, string, string, Exception?> CompletionNotPublished =
			LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(4007, nameof(CompletionNotPublished)),
				"Итерация {Instance}: {EventType} не опубликован (channel закрыт); Running сброшен в finally.");
	}
}
