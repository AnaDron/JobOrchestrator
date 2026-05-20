namespace JobOrchestrator.Abstractions;

/// <summary>
/// Handle одного manual-запуска, возвращённый <see cref="IInstanceHandle.RunAsync"/>.
/// Идентифицирует конкретную итерацию (а не identity инстанса), поэтому <see cref="Completion"/>
/// отражает именно её завершение — не подвержен race с Auto-тиками или параллельными manual-вызовами.
/// </summary>
public interface IIterationHandle {
	/// <summary>Human-readable идентификатор инстанса, для которого запущена итерация (для логов/диагностики).</summary>
	string FullyQualifiedName { get; }

	/// <summary>
	/// Task, завершающийся, когда итерация дошла до terminal-state. Caller работает стандартно
	/// (<c>await</c>, <c>Task.WhenAll</c>, <c>WaitAsync(ct)</c>).
	/// <para>
	/// <b>Семантика исхода:</b>
	/// </para>
	/// <list type="bullet">
	/// <item>
	/// <c>await</c> прошёл без исключения — stage handler отработал успешно.
	/// </item>
	/// <item>
	/// Брошен <see cref="IterationFailedException"/> — итерация не завершилась успехом. Конкретика —
	/// в <see cref="IterationFailedException.Reason"/> (<see cref="IterationFailureReason.StageException"/> —
	/// stage handler бросил business exception (доступен через <c>InnerException</c>);
	/// <see cref="IterationFailureReason.Cancelled"/> — отменена каскадом удаления;
	/// <see cref="IterationFailureReason.Faulted"/> — infrastructure failure (shutdown / event-loop crash)).
	/// </item>
	/// </list>
	/// </summary>
	Task Completion { get; }
}
