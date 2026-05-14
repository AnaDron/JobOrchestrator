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
/// чтобы не двойному dispose-у scanner-а и not double-log shutdown.
/// </para>
/// </summary>
internal sealed class JobOrchestratorHostedService(
	EventLoop eventLoop,
	DueScanner scanner,
	StageRegistry registry,
	IServiceProvider services,
	OrchestratorLifecycle lifecycle,
	ILogger<JobOrchestratorHostedService> logger
) : BackgroundService {
	private readonly Guid _instanceId = Guid.NewGuid();
	private int _stopGate;    // 0 = не остановлен; 1 = StopAsync уже выполняется/выполнен.

	public override async Task StartAsync(CancellationToken cancellationToken) {
		// Fail-fast: ловим misconfiguration на старте, а не на первой итерации стадии.
		registry.ValidateServiceRegistrations(services);
		logger.LogInformation("JobOrchestratorHostedService starting, instance={InstanceId}", _instanceId);
		await base.StartAsync(cancellationToken).ConfigureAwait(false);
	}

	public override async Task StopAsync(CancellationToken cancellationToken) {
		// Идемпотентный StopAsync: первый вызов проходит весь shutdown, повторные — no-op.
		// Без этого: BackgroundService.StopHost + manual.StopAsync дали бы 2 вызова → scanner.Dispose()
		// уже на disposed object → ObjectDisposedException в логе.
		if (Interlocked.Exchange(ref _stopGate, 1) != 0) return;
		await base.StopAsync(cancellationToken).ConfigureAwait(false);
		scanner.Dispose();
		logger.LogInformation("JobOrchestratorHostedService stopped, instance={InstanceId}", _instanceId);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
		var scannerTask = scanner.RunAsync(stoppingToken);
		try {
			await eventLoop.RunAsync(stoppingToken).ConfigureAwait(false);
			// Штатный shutdown — закрываем channel, чтобы внешние вызовы получили fail-fast вместо подвисания.
			lifecycle.CloseChannel();
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			lifecycle.CloseChannel();
		} catch (Exception ex) {
			logger.LogCritical(ex, "JobOrchestrator event loop crashed; оркестрация отключена до рестарта приложения.");
			lifecycle.MarkFaulted();
			// НЕ throw — иначе BackgroundService.StopHost остановит весь хост.
		} finally {
			// Ждём, пока DueScanner завершится (stoppingToken его уже остановит).
			try {
				await scannerTask.ConfigureAwait(false);
			} catch (Exception ex) when (ex is not OperationCanceledException) {
				logger.LogWarning(ex, "DueScanner завершился с ошибкой.");
			}
		}
	}
}
