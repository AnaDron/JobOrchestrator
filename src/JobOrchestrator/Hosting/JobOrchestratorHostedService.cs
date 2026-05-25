using JobOrchestrator.Configuration;
using JobOrchestrator.Configuration.Internal;
using JobOrchestrator.Internal;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Hosting;

/// <summary>
/// Стартует <see cref="EventLoop"/> как BackgroundService (event-loop + due-scan-loop крутятся
/// внутри одного RunAsync через Task.WhenAll). Перехватывает крах в <c>LogCritical</c> и выставляет
/// <see cref="JobOrchestratorRuntime.MarkFaulted"/> — <see cref="IJobOrchestrator"/>-фасад начинает
/// fail-fast для всех внешних вызовов. При штатном shutdown закрывает channel через
/// <see cref="JobOrchestratorRuntime.CloseChannel"/>.
/// <para>
/// <see cref="StopAsync"/> идемпотентен через <c>Interlocked _stopGate</c>: повторный вызов — no-op,
/// чтобы не дублировать shutdown-log.
/// </para>
/// </summary>
internal sealed class JobOrchestratorHostedService(
	EventLoop eventLoop,
	StageRegistry registry,
	IServiceProvider services,
	JobOrchestratorHostOptions hostOptions,
	JobOrchestratorRuntime runtime,
	ILogger<JobOrchestratorHostedService> logger
) : BackgroundService {
	private readonly Guid _instanceId = Guid.NewGuid();
	private int _startGate;
	private int _stopGate;

	public override async Task StartAsync(CancellationToken cancellationToken) {
		if (Interlocked.Exchange(ref _startGate, 1) != 0) return;
		if (Volatile.Read(ref _stopGate) != 0) return;

		ConfigurationValidator.ValidateServiceRegistrations(registry.AllStages, services);
		Log.Starting(logger, _instanceId, null);
		await base.StartAsync(cancellationToken).ConfigureAwait(false);
	}

	public override async Task StopAsync(CancellationToken cancellationToken) {
		if (Interlocked.Exchange(ref _stopGate, 1) != 0) return;
		await base.StopAsync(cancellationToken).ConfigureAwait(false);
		runtime.OnShutdown();
		Log.Stopped(logger, _instanceId, null);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
		try {
			await eventLoop.RunAsync(stoppingToken).ConfigureAwait(false);
			runtime.CloseChannel();
			await ApplyShutdownIterationTimeoutAsync(stoppingToken).ConfigureAwait(false);
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			runtime.CloseChannel();
			await ApplyShutdownIterationTimeoutAsync(stoppingToken).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.EventLoopCrashed(logger, ex);
			runtime.MarkFaulted();
			// НЕ throw — иначе BackgroundService.StopHost остановит весь хост.
		}
	}

	private async Task ApplyShutdownIterationTimeoutAsync(CancellationToken stoppingToken) {
		if (hostOptions.ShutdownIterationTimeout is not { } timeout) return;
		try {
			await Task.Delay(timeout, stoppingToken).ConfigureAwait(false);
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
		}
		runtime.CancelRunningWorkers();
		Log.ShutdownWorkersCancelled(logger, timeout, null);
	}

	/// <summary>Pre-allocated delegates для hot-path логов HostedService. EventId-ы 7xxx.</summary>
	private static class Log {
		public static readonly Action<ILogger, Guid, Exception?> Starting =
			LoggerMessage.Define<Guid>(LogLevel.Information, new EventId(7001, nameof(Starting)),
				"JobOrchestratorHostedService starting, instance={InstanceId}.");

		public static readonly Action<ILogger, Guid, Exception?> Stopped =
			LoggerMessage.Define<Guid>(LogLevel.Information, new EventId(7002, nameof(Stopped)),
				"JobOrchestratorHostedService stopped, instance={InstanceId}.");

		public static readonly Action<ILogger, TimeSpan, Exception?> ShutdownWorkersCancelled =
			LoggerMessage.Define<TimeSpan>(LogLevel.Information, new EventId(7003, nameof(ShutdownWorkersCancelled)),
				"Shutdown: running-итерации отменены после ожидания {Timeout}.");

		public static readonly Action<ILogger, Exception?> EventLoopCrashed =
			LoggerMessage.Define(LogLevel.Critical, new EventId(7004, nameof(EventLoopCrashed)),
				"JobOrchestrator event loop crashed; оркестрация отключена до рестарта приложения.");
	}
}
