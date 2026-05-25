using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Запуск одной итерации инстанса в свежем DI-scope с watchdog-CTS, формирование <see cref="JobContext"/>,
/// разворот logger scope со структурными полями и публикация <see cref="StageCompletedEvent"/> /
/// <see cref="StageFailedEvent"/> в channel. Вызывается из <c>EventLoop.BeginIteration</c> через
/// <see cref="RunIterationSafeAsync"/> (Task.Run на ThreadPool).
/// <para>
/// Различает источники cancel через ОТДЕЛЬНЫЕ CTS:
/// </para>
/// <list type="bullet">
/// <item><c>stoppingToken</c> — shutdown хоста;</item>
/// <item><c>workersToken</c> — crash event loop (<see cref="JobOrchestratorRuntime.MarkFaulted"/>);</item>
/// <item><c>watchdogCts</c> — <see cref="StageDescriptor.ExecutionTimeout"/> превышен;</item>
/// <item><c>cascadeCts</c> (== <see cref="Instance.RunCts"/>) — event-loop отменил из-за cascade-removal.</item>
/// </list>
/// </summary>
internal sealed partial class EventLoop {
	private async Task RunIterationSafeAsync(Instance instance, TriggerSource trigger, Action releaseConcurrency, CancellationToken ct) {
		try {
			await RunIterationAsync(instance, trigger, releaseConcurrency, runtime.WorkersCancellationToken, ct).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.UnhandledIterationFault(logger, instance.FullyQualifiedName, ex);
		}
	}

	/// <summary>
	/// <paramref name="releaseConcurrency"/> вызывается в finally — освобождает per-stage + global семафоры.
	/// <paramref name="workersToken"/> сшивается с shutdown/watchdog/cascade в linked CTS, передаётся в
	/// <see cref="IJobService.ExecuteAsync"/>.
	/// </summary>
	private async Task RunIterationAsync(Instance instance, TriggerSource trigger, Action releaseConcurrency, CancellationToken workersToken, CancellationToken stoppingToken) {
		string correlationId = Guid.NewGuid().ToString("N");
		var logFields = BuildLogScopeFields(instance, correlationId);

		await using var scope = rootProvider.CreateAsyncScope();
		using var loggerScope = _stageLogger.BeginScope(logFields);

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
}
