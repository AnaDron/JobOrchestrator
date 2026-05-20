namespace JobOrchestrator.Abstractions;

/// <summary>
/// Причина, по которой <see cref="IInstanceHandle.RunAsync"/> не смог запустить итерацию.
/// Возвращается через <see cref="IterationRejectedException.Reason"/>. Caller отличает «отказ»
/// от «итерация запущена» по факту наличия handle или исключения; <c>Reason</c> уточняет, почему.
/// </summary>
public enum IterationRejectReason {
	/// <summary>Инстанс не найден в графе на момент попытки запуска (ещё не материализован или удалён каскадом).</summary>
	NotFound,
	/// <summary>Инстанс в каскадном удалении — новый запуск невозможен до finalize.</summary>
	Terminating,
	/// <summary>Итерация уже идёт; параллельный запуск того же инстанса не поддерживается.</summary>
	AlreadyRunning,
	/// <summary>Manual попал в окно <c>Debounce</c> от <c>LastAttempt</c>.</summary>
	Debounced,
	/// <summary>Запуск отложен — исчерпан <c>ConcurrencyLimit</c> стадии или глобальный лимит.</summary>
	ConcurrencyDeferred,
	/// <summary>Event loop оркестратора крашнулся, остановлен или channel закрыт; вызов не может быть обслужен.</summary>
	Faulted,
}
