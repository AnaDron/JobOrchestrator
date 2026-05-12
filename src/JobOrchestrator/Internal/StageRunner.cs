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
	ILoggerFactory loggerFactory
) {
	private readonly ILogger _logger = loggerFactory.CreateLogger("JobOrchestrator.StageRunner");

	public async Task RunIterationAsync(Job job, TriggerSource trigger, CancellationToken stoppingToken) {
		string correlationId = Guid.NewGuid().ToString("N");
		var logFields = BuildLogScopeFields(job, correlationId);

		await using var scope = rootProvider.CreateAsyncScope();
		using var loggerScope = _logger.BeginScope(logFields);

		var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
		if (job.Stage.ExecutionTimeout is { } timeout) {
			runCts.CancelAfter(timeout);
		}
		job.RunCts = runCts;

		try {
			var service = (IJobService)scope.ServiceProvider.GetRequiredService(job.Stage.ServiceType);
			var stateScope = $"{job.Stage.Name}:{job.EncodedKey}";
			var jobState = new DefaultJobState(stateStore, stateScope);

			var jobContext = new JobContext(
				correlationId: correlationId,
				trigger: trigger,
				state: jobState,
				lastSuccessAt: job.LastSuccess,
				dependencyKeys: job.DependencyKeys,
				fullyQualifiedName: job.FullyQualifiedName,
				addKey: k => channel.Writer.TryWrite(new KeyAddedEvent(job.Stage.Name, k)),
				removeKey: k => channel.Writer.TryWrite(new KeyRemovedEvent(job.Stage.Name, k))
			);

			_logger.LogDebug("Старт итерации {Instance} (trigger={Trigger}).", job.FullyQualifiedName, trigger);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			_logger.LogDebug("Итерация {Instance} успешно завершена.", job.FullyQualifiedName);
			channel.Writer.TryWrite(new StageCompletedEvent(job));
		} catch (Exception ex) {
			// Cancellation тоже считается неуспехом (watchdog / shutdown). Эти случаи различаем в log-level.
			if (ex is OperationCanceledException && stoppingToken.IsCancellationRequested) {
				_logger.LogInformation("Итерация {Instance} отменена при shutdown.", job.FullyQualifiedName);
			} else if (ex is OperationCanceledException) {
				_logger.LogWarning("Итерация {Instance} отменена (watchdog timeout или внешний cancel).", job.FullyQualifiedName);
			} else {
				_logger.LogWarning(ex, "Итерация {Instance} завершилась с ошибкой.", job.FullyQualifiedName);
			}
			channel.Writer.TryWrite(new StageFailedEvent(job, ex));
		} finally {
			runCts.Dispose();
			job.RunCts = null;
		}
	}

	private static Dictionary<string, object> BuildLogScopeFields(Job job, string correlationId) {
		Dictionary<string, object> fields = new(StringComparer.Ordinal) {
			["CorrelationId"] = correlationId,
			["FullyQualifiedName"] = job.FullyQualifiedName,
			["StageName"] = job.Stage.Name,
		};
		// Каждый компонент DependencyKeys как отдельное поле {depStageName}Key — для structured Seq-фильтров.
		foreach (var kv in job.DependencyKeys) {
			fields[$"{kv.Key}Key"] = kv.Value;
		}
		return fields;
	}
}
