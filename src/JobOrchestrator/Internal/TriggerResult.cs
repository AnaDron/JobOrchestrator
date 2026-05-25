namespace JobOrchestrator.Internal;

/// <summary>
/// Внутренний результат <c>EventLoop.BeginIteration</c> / <c>EventLoop.TryAcceptTrigger</c>. Public API
/// (<see cref="IInstanceHandle.RunAsync"/>) экспонирует это через <see cref="IIterationHandle"/>
/// (на Started) либо <see cref="IterationRejectedException"/> (на reject).
/// </summary>
internal enum TriggerResult {
	/// <summary>Итерация фактически запущена: <c>TryBeginRunning</c> прошёл, runner поставлен на ThreadPool.</summary>
	Started,
	/// <summary>Инстанс уже исполняется.</summary>
	AlreadyRunning,
	/// <summary>Manual-триггер отвергнут окном дебаунса (от LastAttempt, вне зависимости от исхода последней попытки).</summary>
	Debounced,
	/// <summary>Инстанс с указанным StageName и DependencyKeys не существует.</summary>
	NotFound,
	/// <summary>Инстанс помечен на удаление каскадом (<see cref="InstanceLifecycleState.Terminating"/>).</summary>
	Terminating,
	/// <summary>Auto-триггер в окне retry-delay после неуспеха. Manual игнорирует это окно.</summary>
	WaitingRetry,
	/// <summary>Запуск отложен — исчерпан <c>ConcurrencyLimit</c> стадии или глобальный лимит итераций.</summary>
	ConcurrencyDeferred,
	/// <summary>Event loop оркестратора крашнулся или не стартовал; вызов не может быть обслужен.</summary>
	Faulted,
}
