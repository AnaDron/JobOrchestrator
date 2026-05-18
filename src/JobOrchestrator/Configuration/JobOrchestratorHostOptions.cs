namespace JobOrchestrator.Configuration;

/// <summary>
/// Настройки hosted-service и устойчивости event loop (не per-stage — см. <see cref="JobDefaults"/>).
/// </summary>
public sealed class JobOrchestratorHostOptions {
	/// <summary>Дефолт: после трёх подряд сбоев обработчиков событий — <c>MarkFaulted</c>.</summary>
	public const int DefaultHandlerCrashFaultThreshold = 3;

	/// <summary>
	/// Подряд сбоев <see cref="Internal.EventLoop"/>-handler'ов до перевода оркестратора в fault.
	/// Manual-trigger TCS при любом сбое handler'а завершается исключением.
	/// </summary>
	public int HandlerCrashFaultThreshold { get; set; } = DefaultHandlerCrashFaultThreshold;

	/// <summary>
	/// После <see cref="Internal.OrchestratorLifecycle.CloseChannel"/> при graceful shutdown: максимальное
	/// ожидание завершения running-итераций до <see cref="Internal.OrchestratorLifecycle.CancelRunningWorkers"/>.
	/// <c>null</c> — не форсировать cancel (только <c>stoppingToken</c> хоста).
	/// </summary>
	public TimeSpan? ShutdownIterationTimeout { get; set; }
}
