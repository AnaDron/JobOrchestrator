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

	public async Task RunIterationAsync(StageInstance instance, TriggerSource trigger, CancellationToken stoppingToken) {
		string correlationId = Guid.NewGuid().ToString("N");
		var logFields = BuildLogScopeFields(instance, correlationId);

		await using var scope = rootProvider.CreateAsyncScope();
		using var loggerScope = _logger.BeginScope(logFields);

		var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
		if (instance.Stage.ExecutionTimeout is { } timeout) {
			runCts.CancelAfter(timeout);
		}
		instance.RunCts = runCts;

		try {
			var service = (IJobService)scope.ServiceProvider.GetRequiredService(instance.Stage.ServiceType);
			var jobState = new DefaultJobState(stateStore, instance.StateScope);

			var jobContext = new JobContext(
				correlationId: correlationId,
				trigger: trigger,
				state: jobState,
				lastSuccessAt: instance.LastSuccess,
				dependencyKeys: instance.DependencyKeys,
				fullyQualifiedName: instance.FullyQualifiedName,
				addKey: k => channel.Writer.TryWrite(new KeyAddedEvent(instance.Stage.Name, k)),
				removeKey: k => channel.Writer.TryWrite(new KeyRemovedEvent(instance.Stage.Name, k))
			);

			_logger.LogDebug("Старт итерации {Instance} (trigger={Trigger}).", instance.FullyQualifiedName, trigger);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			_logger.LogDebug("Итерация {Instance} успешно завершена.", instance.FullyQualifiedName);
			channel.Writer.TryWrite(new StageCompletedEvent(instance));
		} catch (Exception ex) {
			// Cancellation тоже считается неуспехом (watchdog / shutdown). Эти случаи различаем в log-level.
			if (ex is OperationCanceledException && stoppingToken.IsCancellationRequested) {
				_logger.LogInformation("Итерация {Instance} отменена при shutdown.", instance.FullyQualifiedName);
			} else if (ex is OperationCanceledException) {
				_logger.LogWarning("Итерация {Instance} отменена (watchdog timeout или внешний cancel).", instance.FullyQualifiedName);
			} else {
				_logger.LogWarning(ex, "Итерация {Instance} завершилась с ошибкой.", instance.FullyQualifiedName);
			}
			channel.Writer.TryWrite(new StageFailedEvent(instance, ex));
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
