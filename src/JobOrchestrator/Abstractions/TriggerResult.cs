namespace JobOrchestrator.Abstractions;

/// <summary>Результат попытки запуска инстанса (<see cref="IInstanceHandle.TriggerAsync"/> или внутренний Auto-тик).</summary>
public enum TriggerResult {
	/// <summary>
	/// Итерация фактически запущена: <c>TryBeginRunning</c> прошёл, runner поставлен на ThreadPool.
	/// Для <see cref="IInstanceHandle.TriggerAsync"/> — только после успешного <c>BeginIteration</c>
	/// (не путать с внутренним <see cref="TriggerResult.Accepted"/>).
	/// </summary>
	Started,
	/// <summary>
	/// Политика триггера пройдена (retry/debounce/running), но итерация ещё не начата.
	/// Внутренний gate для event loop; <see cref="IInstanceHandle.TriggerAsync"/> это значение не возвращает.
	/// </summary>
	Accepted,
	/// <summary>Инстанс уже исполняется.</summary>
	AlreadyRunning,
	/// <summary>Manual-триггер отвергнут окном дебаунса (от LastAttempt, вне зависимости от исхода последней попытки).</summary>
	Debounced,
	/// <summary>Инстанс с указанным StageName и DependencyKeys не существует.</summary>
	NotFound,
	/// <summary>
	/// Инстанс помечен на удаление каскадом (<see cref="InstanceLifecycleState.Terminating"/>);
	/// новый запуск невозможен до finalize.
	/// </summary>
	Terminating,
	/// <summary>Auto-триггер в окне retry-delay после неуспеха. Manual игнорирует это окно.</summary>
	WaitingRetry,
	/// <summary>
	/// Набор имён в <c>DependencyKeys</c> не соответствует <c>DependsOnInstance</c>-зависимостям стадии.
	/// Передан wrong-key-set (например, переданы ключи которых стадия не требует, или отсутствуют требуемые).
	/// </summary>
	InvalidKeys,
	/// <summary>Event loop оркестратора крашнулся или не стартовал; вызов не может быть обслужен.</summary>
	Faulted,
}
