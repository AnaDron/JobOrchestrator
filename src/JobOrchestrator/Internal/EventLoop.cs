using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Single-threaded consumer событий оркестратора. Все state-transitions инстансов происходят здесь;
/// итерации запускаются на ThreadPool через <see cref="StageRunner"/>.
/// </summary>
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
			case KeyAddedEvent ka: HandleKeyAdded(ka.StageName, ka.Key); break;
			case KeyRemovedEvent kr: await HandleKeyRemovedAsync(kr.StageName, kr.Key, ct).ConfigureAwait(false); break;
			case StageCompletedEvent sc: HandleStageCompleted(sc.Instance); break;
			case StageFailedEvent sf: HandleStageFailed(sf.Instance, sf.Exception); break;
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
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		_ = Task.Run(() => runner.RunIterationAsync(instance, trigger, ct), ct);
	}

	private void HandleKeyAdded(string stageName, string key) {
		if (!keyspace.Add(stageName, key)) return;
		// Каждая стадия с DependsOnInstance(stageName) может получить новый инстанс.
		foreach (var dependent in registry.StagesDependingOnInstance(stageName))
			CreateAndStart(dependent);
	}

	private async Task HandleKeyRemovedAsync(string stageName, string key, CancellationToken ct) {
		if (!keyspace.Remove(stageName, key)) return;

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

		foreach (var instance in affected) {
			if (instance.State == InstanceLifecycleState.Running && instance.RunCts is { } cts) {
				try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
			}
			if (_timers.TryGetValue(instance, out var timer)) {
				timer.Dispose();
				_timers.Remove(instance);
			}
			instances.Remove(instance);
			try {
				await stateStore.RemoveScopeAsync(instance.StateScope, ct).ConfigureAwait(false);
			} catch (Exception ex) {
				logger.LogWarning(ex, "RemoveScopeAsync для {Instance} завершился с ошибкой.", instance.FullyQualifiedName);
			}
		}
	}

	private void HandleStageCompleted(StageInstance instance) {
		bool wasFirstSuccess = !instance.LastSuccess.HasValue;
		var now = _timeProvider.GetUtcNow();
		instance.LastAttempt = now;
		instance.LastSuccess = now;       // монотонно: не сбрасывается на последующих неуспехах
		instance.ConsecutiveFailures = 0;
		instance.LastError = null;
		instance.State = InstanceLifecycleState.Idle;
		ScheduleNextTick(instance, instance.Stage.Interval);

		if (wasFirstSuccess) {
			// Каскад: возможно теперь разрешаются зависимости других стадий.
			foreach (var dependent in EnumerateDirectDependents(instance.Stage.Name))
				CreateAndStart(dependent);
		}
	}

	private void HandleStageFailed(StageInstance instance, Exception ex) {
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

	private void ScheduleNextTick(StageInstance instance, TimeSpan delay) {
		instance.NextTickAtMs = Environment.TickCount64 + (long)delay.TotalMilliseconds;
		if (_timers.TryGetValue(instance, out var timer))
			timer.ScheduleAt(instance.NextTickAtMs);
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = creator.EvaluateAndCreate(stage);
		foreach (var instance in created)
			StartTimerAndScheduleImmediate(instance);
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
		return ValueTask.CompletedTask;
	}
}
