namespace JobOrchestrator.Abstractions;

/// <summary>Результат попытки запуска инстанса (<see cref="IJobOrchestrator.TriggerAsync"/> или внутренний Auto-тик).</summary>
public enum TriggerResult {
	/// <summary>Триггер принят, итерация поставлена в очередь.</summary>
	Started,
	/// <summary>Инстанс уже исполняется.</summary>
	AlreadyRunning,
	/// <summary>Manual-триггер отвергнут окном дебаунса (от LastAttempt, вне зависимости от исхода последней попытки).</summary>
	Debounced,
	/// <summary>Инстанс с указанным StageName и DependencyKeys не существует.</summary>
	NotFound,
	/// <summary>Auto-триггер в окне retry-delay после неуспеха. Manual игнорирует это окно.</summary>
	WaitingRetry,
	/// <summary>Event loop оркестратора крашнулся или не стартовал; вызов не может быть обслужен.</summary>
	Faulted,
}
