namespace JobOrchestrator.Configuration;

/// <summary>
/// Настройки hosted-service и устойчивости event loop (не per-stage — см. <see cref="JobDefaults"/>).
/// </summary>
public sealed class JobOrchestratorHostOptions {
	/// <summary>Дефолт: после трёх подряд сбоев обработчиков событий — <c>MarkFaulted</c>.</summary>
	public const int DefaultHandlerCrashFaultThreshold = 3;

	/// <summary>
	/// Подряд сбоев <see cref="Internal.EventLoop"/>-handler'ов до перевода оркестратора в fault
	/// (для событий, не покрытых <see cref="FaultOnStateMutatingHandlerCrash"/>).
	/// Manual-trigger TCS при любом сбое handler'а завершается исключением.
	/// </summary>
	public int HandlerCrashFaultThreshold { get; set; } = DefaultHandlerCrashFaultThreshold;

	/// <summary>
	/// При <c>true</c> (по умолчанию) первый сбой handler'а на событии, мутирующем граф/keyspace/метрики
	/// (AddKey/RemoveKey, completion, auto-tick), сразу вызывает fault — иначе возможно частичное
	/// состояние (ключ в keyspace без cascade и т.п.).
	/// </summary>
	public bool FaultOnStateMutatingHandlerCrash { get; set; } = true;

	/// <summary>
	/// Случайный разброс (0..N мс) к отложенному <c>NextAutoUtc</c> при <see cref="TriggerResult.ConcurrencyDeferred"/>,
	/// чтобы due-scan-loop не будил все deferred-инстансы в одну миллисекунду. <c>0</c> — без jitter.
	/// </summary>
	public int ConcurrencyDeferJitterMaxMilliseconds { get; set; } = 500;

	/// <summary>
	/// После <see cref="Internal.JobOrchestratorRuntime.CloseChannel"/> при graceful shutdown: максимальное
	/// ожидание завершения running-итераций до <see cref="Internal.JobOrchestratorRuntime.CancelRunningWorkers"/>.
	/// <c>null</c> — не форсировать cancel (только <c>stoppingToken</c> хоста).
	/// </summary>
	public TimeSpan? ShutdownIterationTimeout { get; set; }
}
