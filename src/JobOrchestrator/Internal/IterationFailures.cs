namespace JobOrchestrator.Internal;

/// <summary>
/// Centralized factory для <see cref="IterationFailedException"/>: единый источник message-формулировок
/// и InnerException-политики. Принимает <see cref="Instance"/> — call-sites в event-loop'е получают
/// короткий вызов <c>IterationFailures.X(instance)</c> вместо разворачивания FQN на каждом сайте.
/// </summary>
internal static class IterationFailures {
	/// <summary>
	/// Cancelled-исход через каскадное удаление инстанса. InnerException намеренно null — внутренний
	/// OCE от cascade-CTS не несёт диагностической ценности, а единый отсутствующий inner
	/// гарантирует одинаковое поведение независимо от race-тайминга (runner-success vs runner-cancelled
	/// до cascade).
	/// </summary>
	public static IterationFailedException CancelledByCascade(Instance instance) =>
		new(IterationFailureReason.Cancelled, instance.FullyQualifiedName,
			$"Итерация {instance.FullyQualifiedName} прервана: инстанс удалён каскадом.");

	/// <summary>
	/// Stage handler (IJobService.ExecuteAsync) бросил исключение. Оригинал сохраняется через
	/// <see cref="System.Exception.InnerException"/>.
	/// </summary>
	public static IterationFailedException StageHandlerFailed(Instance instance, Exception stageException) =>
		new(IterationFailureReason.StageException, instance.FullyQualifiedName,
			$"Итерация {instance.FullyQualifiedName} завершилась с ошибкой stage handler'а.", stageException);

	/// <summary>
	/// Infrastructure failure: оркестратор остановлен / event-loop handler крашнулся / channel закрыт
	/// ДО публикации completion-события. Причина (shutdown reason / handler exception) — через
	/// <see cref="System.Exception.InnerException"/>.
	/// </summary>
	public static IterationFailedException OrchestratorShutdown(Instance instance, Exception shutdownCause) =>
		new(IterationFailureReason.Faulted, instance.FullyQualifiedName,
			$"Итерация {instance.FullyQualifiedName} прервана: оркестратор остановлен.", shutdownCause);
}
