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

			Log.IterationStart(_logger, instance.FullyQualifiedName, trigger, null);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			Log.IterationCompleted(_logger, instance.FullyQualifiedName, null);
			channel.Writer.Publish(new StageCompletedEvent(instance, time.GetUtcNow()));
		} catch (Exception ex) {
			// Cancellation тоже считается неуспехом (watchdog / shutdown). Эти случаи различаем в log-level.
			if (ex is OperationCanceledException && stoppingToken.IsCancellationRequested) {
				Log.IterationCancelledShutdown(_logger, instance.FullyQualifiedName, null);
			} else if (ex is OperationCanceledException) {
				Log.IterationCancelledWatchdog(_logger, instance.FullyQualifiedName, null);
			} else {
				Log.IterationFailed(_logger, instance.FullyQualifiedName, ex);
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

	/// <summary>
	/// Pre-allocated <see cref="LoggerMessage.Define{T}"/>-делегаты для hot-path логов runner-а
	/// (по 1-2 сообщения на каждую итерацию любой стадии). EventId-ы 4xxx — диапазон StageRunner.
	/// </summary>
	private static class Log {
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
				"Итерация {Instance} отменена (watchdog timeout или внешний cancel).");

		public static readonly Action<ILogger, string, Exception?> IterationFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(4005, nameof(IterationFailed)),
				"Итерация {Instance} завершилась с ошибкой.");
	}
}
