using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Запускает одну итерацию инстанса в свежем DI-scope с watchdog-CTS, формирует <see cref="JobContext"/>,
/// разворачивает logger scope со структурными полями и публикует <see cref="StageCompletedEvent"/> или <see cref="StageFailedEvent"/>.
/// </summary>
internal sealed class StageRunner(
	IServiceProvider rootProvider,
	IJobStateStore stateStore,
	Channel<OrchestratorEvent> channel,
	OrchestratorLifecycle lifecycle,
	TimeProvider time,
	ILoggerFactory loggerFactory
) {
	private readonly ILogger _logger = loggerFactory.CreateLogger("JobOrchestrator.StageRunner");

	public async Task RunIterationAsync(StageInstance instance, TriggerSource trigger, CancellationToken stoppingToken) {
		string correlationId = Guid.NewGuid().ToString("N");
		var logFields = BuildLogScopeFields(instance, correlationId);

		await using var scope = rootProvider.CreateAsyncScope();
		using var loggerScope = _logger.BeginScope(logFields);

		// Linked CTS: cancel при host shutdown (stoppingToken) И при crash event loop (lifecycle.WorkersCancellationToken).
		var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifecycle.WorkersCancellationToken);
		if (instance.Stage.ExecutionTimeout is { } timeout) {
			runCts.CancelAfter(timeout);
		}
		instance.RunCts = runCts;

		try {
			var service = (IJobService)scope.ServiceProvider.GetRequiredService(instance.Stage.ServiceType);
			var jobState = new DefaultJobState(stateStore, instance.StateScope);
			var sink = new ChannelJobContextSink(channel.Writer, instance);

			var jobContext = new JobContext {
				CorrelationId = correlationId,
				Trigger = trigger,
				State = jobState,
				LastSuccessAt = instance.LastSuccess,
				DependencyKeys = instance.DependencyKeys,
				FullyQualifiedName = instance.FullyQualifiedName,
				Sink = sink,
			};

			_logger.LogDebug("Старт итерации {Instance} (trigger={Trigger}).", instance.FullyQualifiedName, trigger);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			_logger.LogDebug("Итерация {Instance} успешно завершена.", instance.FullyQualifiedName);
			channel.Writer.Publish(new StageCompletedEvent(instance, time.GetUtcNow()));
		} catch (Exception ex) {
			// Cancellation тоже считается неуспехом (watchdog / shutdown). Эти случаи различаем в log-level.
			if (ex is OperationCanceledException && stoppingToken.IsCancellationRequested) {
				_logger.LogInformation("Итерация {Instance} отменена при shutdown.", instance.FullyQualifiedName);
			} else if (ex is OperationCanceledException) {
				_logger.LogWarning("Итерация {Instance} отменена (watchdog timeout или внешний cancel).", instance.FullyQualifiedName);
			} else {
				_logger.LogWarning(ex, "Итерация {Instance} завершилась с ошибкой.", instance.FullyQualifiedName);
			}
			channel.Writer.Publish(new StageFailedEvent(instance, ex, time.GetUtcNow()));
		} finally {
			runCts.Dispose();
			instance.RunCts = null;
		}
	}

	private static Dictionary<string, object> BuildLogScopeFields(StageInstance instance, string correlationId) {
		Dictionary<string, object> fields = new(3 + instance.DependencyKeys.Count, StringComparer.Ordinal) {
			["CorrelationId"] = correlationId,
			["FullyQualifiedName"] = instance.FullyQualifiedName,
			["StageName"] = instance.Stage.Name,
		};
		// Каждый компонент DependencyKeys как отдельное поле {depStageName}Key — для structured Seq-фильтров.
		foreach (var kv in instance.DependencyKeys) {
			fields[$"{kv.Key}Key"] = kv.Value;
		}
		return fields;
	}
}
