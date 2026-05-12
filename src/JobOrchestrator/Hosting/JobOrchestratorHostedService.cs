using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Hosting;

/// <summary>
/// Стартует <see cref="EventLoop"/> как BackgroundService. Перехватывает крах event loop'а
/// в <c>LogCritical</c> и НЕ пере-throw-ит — иначе <see cref="BackgroundService"/> останавил бы весь хост.
/// </summary>
internal sealed class JobOrchestratorHostedService(
	EventLoop eventLoop,
	ILogger<JobOrchestratorHostedService> logger
) : BackgroundService {
	private readonly Guid _instanceId = Guid.NewGuid();

	public override async Task StartAsync(CancellationToken cancellationToken) {
		logger.LogInformation("JobOrchestratorHostedService starting, instance={InstanceId}", _instanceId);
		await base.StartAsync(cancellationToken).ConfigureAwait(false);
	}

	public override async Task StopAsync(CancellationToken cancellationToken) {
		await base.StopAsync(cancellationToken).ConfigureAwait(false);
		logger.LogInformation("JobOrchestratorHostedService stopped, instance={InstanceId}", _instanceId);
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
		try {
			await eventLoop.RunAsync(stoppingToken).ConfigureAwait(false);
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			// Нормальный shutdown.
		} catch (Exception ex) {
			logger.LogCritical(ex, "JobOrchestrator event loop crashed; оркестрация отключена до рестарта приложения.");
			// НЕ throw — иначе BackgroundService.StopHost остановит весь хост.
		}
	}
}
