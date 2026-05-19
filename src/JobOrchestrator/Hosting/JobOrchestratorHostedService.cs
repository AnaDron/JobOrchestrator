using JobOrchestrator.Configuration;
using JobOrchestrator.Configuration.Internal;
using JobOrchestrator.Internal;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Hosting;

/// <summary>
/// Стартует <see cref="EventLoop"/> и <see cref="DueScanner"/> как concurrent-tasks BackgroundService.
/// Перехватывает крах любого из них в <c>LogCritical</c> и выставляет <see cref="OrchestratorLifecycle.MarkFaulted"/> —
/// <see cref="IJobOrchestrator"/>-фасад начинает fail-fast для всех внешних вызовов.
/// При штатном shutdown закрывает channel через <see cref="OrchestratorLifecycle.CloseChannel"/>.
/// <para>
/// <see cref="StopAsync"/> идемпотентен через <c>Interlocked _stopGate</c>: повторный вызов — no-op,
/// чтобы не дублировать shutdown-log.
/// </para>
/// </summary>
internal sealed class JobOrchestratorHostedService(
	EventLoop eventLoop,
	DueScanner scanner,
	StageRegistry registry,
	IServiceProvider services,
	OrchestratorLifecycle lifecycle,
	JobOrchestratorHostOptions hostOptions,
	ILogger<JobOrchestratorHostedService> logger
) : BackgroundService {
	private readonly Guid _instanceId = Guid.NewGuid();
	private int _stopGate;    // 0 = не остановлен; 1 = StopAsync уже выполняется/выполнен.

	public override async Task StartAsync(CancellationToken cancellationToken) {
		// Fail-fast: ловим misconfiguration на старте, а не на первой итерации стадии.
		ConfigurationValidator.ValidateServiceRegistrations(registry.AllStages, services);
		Log.Starting(logger, _instanceId, null);
		await base.StartAsync(cancellationToken).ConfigureAwait(false);
	}

	public override async Task StopAsync(CancellationToken cancellationToken) {
		// Идемпотентный StopAsync: первый вызов проходит весь shutdown, повторные — no-op.
		// Без этого: BackgroundService.StopHost + manual.StopAsync дали бы 2 вызова → дублирующая
		// строка в логе stopped.
		if (Interlocked.Exchange(ref _stopGate, 1) != 0) return;
		await base.StopAsync(cancellationToken).ConfigureAwait(false);
		Log.Stopped(logger, _instanceId, null);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
		var scannerTask = scanner.RunAsync(stoppingToken);
		try {
			await eventLoop.RunAsync(stoppingToken).ConfigureAwait(false);
			// Штатный shutdown — закрываем channel, чтобы внешние вызовы получили fail-fast вместо подвисания.
			lifecycle.CloseChannel();
			await ApplyShutdownIterationTimeoutAsync(stoppingToken).ConfigureAwait(false);
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			lifecycle.CloseChannel();
			await ApplyShutdownIterationTimeoutAsync(stoppingToken).ConfigureAwait(false);
		} catch (Exception ex) {
			Log.EventLoopCrashed(logger, ex);
			lifecycle.MarkFaulted();
			// НЕ throw — иначе BackgroundService.StopHost остановит весь хост.
		} finally {
			// Ждём, пока DueScanner завершится (stoppingToken его уже остановит).
			try {
				await scannerTask.ConfigureAwait(false);
			} catch (Exception ex) when (ex is not OperationCanceledException) {
				Log.DueScannerFaulted(logger, ex);
			}
		}
	}

	private async Task ApplyShutdownIterationTimeoutAsync(CancellationToken stoppingToken) {
		if (hostOptions.ShutdownIterationTimeout is not { } timeout) return;
		try {
			await Task.Delay(timeout, stoppingToken).ConfigureAwait(false);
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			// Host уже отменяет stoppingToken — всё равно форсируем cancel workers ниже.
		}
		lifecycle.CancelRunningWorkers();
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

		public static readonly Action<ILogger, Exception?> DueScannerFaulted =
			LoggerMessage.Define(LogLevel.Warning, new EventId(7005, nameof(DueScannerFaulted)),
				"DueScanner завершился с ошибкой.");
	}
}
