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

		// Различаем источники cancel через ОТДЕЛЬНЫЕ CTS:
		// - stoppingToken — shutdown хоста;
		// - lifecycle.WorkersCancellationToken — crash event loop;
		// - watchdogCts — ExecutionTimeout превышен;
		// - cascadeCts (instance.RunCts) — событие-loop отменил из-за cascade-removal.
		// runCts — linked-источник всех вышеперечисленных, передаётся в IJobService.ExecuteAsync.
		var watchdogCts = instance.Stage.ExecutionTimeout is { } timeout
			? new CancellationTokenSource(timeout)
			: null;
		var cascadeCts = new CancellationTokenSource();
		var runCts = watchdogCts is not null
			? CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifecycle.WorkersCancellationToken, watchdogCts.Token, cascadeCts.Token)
			: CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, lifecycle.WorkersCancellationToken, cascadeCts.Token);
		// EventLoop вызывает Cancel() на cascadeCts (через instance.RunCts) для cascade-removal.
		instance.RunCts = cascadeCts;

		try {
			var service = (IJobService)scope.ServiceProvider.GetRequiredService(instance.Stage.ServiceType);
			var jobState = new DefaultJobState(stateStore, instance.StateScope);

			var jobContext = new JobContext {
				CorrelationId = correlationId,
				Trigger = trigger,
				State = jobState,
				LastSuccessAt = instance.Metrics.LastSuccess,
				DependencyKeys = instance.DependencyKeys,
				FullyQualifiedName = instance.FullyQualifiedName,
				Sink = instance.Sink,    // pre-allocated в InstanceCreator, переиспользуется через все итерации
			};

			Log.IterationStart(_logger, instance.FullyQualifiedName, trigger, null);
			await service.ExecuteAsync(jobContext, runCts.Token).ConfigureAwait(false);
			Log.IterationCompleted(_logger, instance.FullyQualifiedName, null);
			channel.Writer.Publish(new StageCompletedEvent(instance, time.GetUtcNow()));
		} catch (OperationCanceledException oce) {
			// Различаем источник cancel — даёт точный StageFailed.Exception для подписчика.
			Exception failure;
			if (stoppingToken.IsCancellationRequested) {
				Log.IterationCancelledShutdown(_logger, instance.FullyQualifiedName, null);
				failure = oce;
			} else if (watchdogCts?.IsCancellationRequested == true) {
				Log.IterationCancelledWatchdog(_logger, instance.FullyQualifiedName, null);
				failure = new TimeoutException(
					$"Стадия {instance.FullyQualifiedName} превысила ExecutionTimeout ({instance.Stage.ExecutionTimeout}).",
					oce);
			} else if (cascadeCts.IsCancellationRequested) {
				Log.IterationCancelledCascade(_logger, instance.FullyQualifiedName, null);
				failure = oce;
			} else {
				// Внутренний OCE сервиса, не связанный с нашими CTS.
				Log.IterationFailed(_logger, instance.FullyQualifiedName, oce);
				failure = oce;
			}
			channel.Writer.Publish(new StageFailedEvent(instance, failure, time.GetUtcNow()));
		} catch (Exception ex) {
			Log.IterationFailed(_logger, instance.FullyQualifiedName, ex);
			channel.Writer.Publish(new StageFailedEvent(instance, ex, time.GetUtcNow()));
		} finally {
			instance.RunCts = null;
			runCts.Dispose();
			watchdogCts?.Dispose();
			cascadeCts.Dispose();
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
				"Итерация {Instance} превысила ExecutionTimeout (watchdog).");

		public static readonly Action<ILogger, string, Exception?> IterationCancelledCascade =
			LoggerMessage.Define<string>(LogLevel.Information, new EventId(4006, nameof(IterationCancelledCascade)),
				"Итерация {Instance} отменена при cascade-removal.");

		public static readonly Action<ILogger, string, Exception?> IterationFailed =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(4005, nameof(IterationFailed)),
				"Итерация {Instance} завершилась с ошибкой.");
	}
}
