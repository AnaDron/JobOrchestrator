namespace JobOrchestrator.Abstractions;

/// <summary>
/// Причина, по которой <see cref="IIterationHandle.Completion"/> резолвится в failure (Task.IsFaulted).
/// Возвращается через <see cref="IterationFailedException.Reason"/>. Caller pattern-match'ит по нему
/// для разделения «бизнес-логика стадии упала» / «инстанс отменён каскадом» / «инфраструктурный сбой».
/// </summary>
public enum IterationFailureReason {
	/// <summary>
	/// Stage handler (<c>IJobService.ExecuteAsync</c>) бросил исключение. Оригинал доступен через
	/// <see cref="System.Exception.InnerException"/>.
	/// </summary>
	StageException,
	/// <summary>
	/// Итерация прервана каскадным удалением инстанса (родительский ключ удалён через
	/// <c>UnregisterKey</c> или <c>ctx.RemoveKeyAsync</c>). Штатное событие — caller узнаёт, что
	/// именно эта итерация не дошла до завершения.
	/// <para>
	/// Покрывает и race-окно «runner успешно завершился, но event-loop уже видит инстанс в
	/// <c>Terminating</c>»: caller получает <c>Cancelled</c>, потому что инстанс ушёл из системы
	/// и эффект итерации не наблюдаем (даже если stage handler формально отработал).
	/// </para>
	/// </summary>
	Cancelled,
	/// <summary>
	/// Infrastructure failure: оркестратор остановлен, channel закрыт ДО публикации completion-события,
	/// event-loop handler крашнулся. Причина — в <see cref="System.Exception.InnerException"/> (если есть).
	/// </summary>
	Faulted,
}
