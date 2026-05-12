using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Single-threaded consumer событий оркестратора. Все state-transitions инстансов происходят здесь;
/// итерации запускаются на ThreadPool через <see cref="StageRunner"/>.
/// </summary>
internal sealed class EventLoop(
	StageRegistry registry,
	JobManager jobs,
	KeyspaceRegistry keyspace,
	InstanceCreator creator,
	StageRunner runner,
	Channel<OrchestratorEvent> channel,
	IJobStateStore stateStore,
	ILogger<EventLoop> logger,
	TimeProvider? timeProvider = null
) : IAsyncDisposable {
	private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
	private readonly Dictionary<Job, JobTimer> _timers = [];

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
			case TimerTickedEvent tt: HandleTimerTick(tt.Job, ct); break;
			case ManualTriggerRequestedEvent mtr: HandleManualTrigger(mtr, ct); break;
			case KeyAddedEvent ka: HandleKeyAdded(ka.StageName, ka.Key); break;
			case KeyRemovedEvent kr: await HandleKeyRemovedAsync(kr.StageName, kr.Key, ct).ConfigureAwait(false); break;
			case StageCompletedEvent sc: HandleStageCompleted(sc.Job); break;
			case StageFailedEvent sf: HandleStageFailed(sf.Job, sf.Exception); break;
			default:
				logger.LogWarning("Неизвестный тип события: {EventType}", evt.GetType().Name);
				break;
		}
	}

	private void HandleTimerTick(Job job, CancellationToken ct) {
		// Инстанс мог быть удалён до срабатывания timer'а — проверим существование.
		if (!_timers.ContainsKey(job)) return;
		var now = _timeProvider.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(job, TriggerSource.Auto, now);
		if (decision == TriggerResult.Started)
			BeginIteration(job, TriggerSource.Auto, ct);
		// WaitingRetry/AlreadyRunning — ничего; timer перепланируется в StageCompleted/Failed handler-е.
	}

	private void HandleManualTrigger(ManualTriggerRequestedEvent evt, CancellationToken ct) {
		var job = jobs.Find(evt.StageName, evt.DependencyKeys);
		if (job is null) {
			evt.Tcs.TrySetResult(TriggerResult.NotFound);
			return;
		}
		var now = _timeProvider.GetUtcNow();
		var decision = TriggerAcceptance.TryAccept(job, TriggerSource.Manual, now);
		evt.Tcs.TrySetResult(decision);
		if (decision == TriggerResult.Started)
			BeginIteration(job, TriggerSource.Manual, ct);
	}

	private void BeginIteration(Job job, TriggerSource trigger, CancellationToken ct) {
		job.State = JobLifecycleState.Running;
		// Fire-and-forget на ThreadPool: StageRunner внутри публикует StageCompleted/Failed в Channel.
		_ = Task.Run(() => runner.RunIterationAsync(job, trigger, ct), ct);
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

		var affectedJobs = affectedStages
			.SelectMany(s => jobs.InstancesOf(s.Name))
			.Where(inst => inst.DependencyKeys.TryGetValue(stageName, out var v) && string.Equals(v, key, StringComparison.Ordinal))
			.ToList();
		if (affectedJobs.Count == 0) return;

		// Топологически обратный порядок (листья перед корнями).
		var sortedStages = registry.TopologicalSortReverse(affectedStages);
		var stageOrderIndex = sortedStages
			.Select((s, i) => (s.Name, Index: i))
			.ToDictionary(t => t.Name, t => t.Index, StringComparer.Ordinal);
		affectedJobs.Sort((a, b) => stageOrderIndex[a.Stage.Name].CompareTo(stageOrderIndex[b.Stage.Name]));

		foreach (var job in affectedJobs) {
			if (job.State == JobLifecycleState.Running && job.RunCts is { } cts) {
				try { cts.Cancel(); } catch (ObjectDisposedException) { /* race на завершение */ }
			}
			if (_timers.TryGetValue(job, out var timer)) {
				timer.Dispose();
				_timers.Remove(job);
			}
			jobs.Remove(job);
			try {
				await stateStore.RemoveScopeAsync(job.StateScope, ct).ConfigureAwait(false);
			} catch (Exception ex) {
				logger.LogWarning(ex, "RemoveScopeAsync для {Instance} завершился с ошибкой.", job.FullyQualifiedName);
			}
		}
	}

	private void HandleStageCompleted(Job job) {
		bool wasFirstSuccess = !job.LastSuccess.HasValue;
		var now = _timeProvider.GetUtcNow();
		job.LastAttempt = now;
		job.LastSuccess = now;       // монотонно: не сбрасывается на последующих неуспехах
		job.ConsecutiveFailures = 0;
		job.LastError = null;
		job.State = JobLifecycleState.Idle;
		ScheduleNextTick(job, job.Stage.Interval);

		if (wasFirstSuccess) {
			// Каскад: возможно теперь разрешаются зависимости других стадий.
			foreach (var dependent in EnumerateDirectDependents(job.Stage.Name))
				CreateAndStart(dependent);
		}
	}

	private void HandleStageFailed(Job job, Exception ex) {
		var now = _timeProvider.GetUtcNow();
		job.LastAttempt = now;
		job.ConsecutiveFailures++;
		job.LastError = ex.Message;
		// LastSuccess НЕ меняется — монотонная метка.
		job.State = JobLifecycleState.Idle;

		var retryDelay = job.Stage.RetryPolicy.ComputeDelay(job.ConsecutiveFailures);
		var nextDelay = retryDelay > TimeSpan.Zero ? retryDelay : job.Stage.Interval;
		ScheduleNextTick(job, nextDelay);
	}

	private void ScheduleNextTick(Job job, TimeSpan delay) {
		job.NextTickAtMs = Environment.TickCount64 + (long)delay.TotalMilliseconds;
		if (_timers.TryGetValue(job, out var timer))
			timer.ScheduleAt(job.NextTickAtMs);
	}

	private void CreateAndStart(StageDescriptor stage) {
		var created = creator.EvaluateAndCreate(stage);
		foreach (var job in created)
			StartTimerAndScheduleImmediate(job);
	}

	private void StartTimerAndScheduleImmediate(Job job) {
		var timer = new JobTimer(job, channel.Writer);
		_timers[job] = timer;
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
