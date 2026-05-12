using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Hosting;

/// <summary>
/// Стартует <see cref="EventLoop"/> как BackgroundService. Перехватывает крах event loop'а
/// в <c>LogCritical</c> и выставляет <see cref="OrchestratorLifecycle.MarkFaulted"/> —
/// <see cref="IJobOrchestrator"/>-фасад начинает fail-fast для всех внешних вызовов.
/// При штатном shutdown закрывает channel через <see cref="OrchestratorLifecycle.CloseChannel"/>.
/// </summary>
internal sealed class JobOrchestratorHostedService(
	EventLoop eventLoop,
	OrchestratorLifecycle lifecycle,
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
			// Штатный shutdown — закрываем channel, чтобы внешние вызовы получили fail-fast вместо подвисания.
			lifecycle.CloseChannel();
		} catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
			lifecycle.CloseChannel();
		} catch (Exception ex) {
			logger.LogCritical(ex, "JobOrchestrator event loop crashed; оркестрация отключена до рестарта приложения.");
			lifecycle.MarkFaulted();
			// НЕ throw — иначе BackgroundService.StopHost остановит весь хост.
		}
	}
}
