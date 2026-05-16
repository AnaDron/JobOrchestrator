namespace JobOrchestrator.Abstractions;

/// <summary>
/// Категория исхода одного цикла стадии: успех, отказ или отмена каскадом удаления инстанса.
/// </summary>
public enum StageOutcomeKind {
	/// <summary>Стадия отработала без исключений.</summary>
	Success,
	/// <summary>Стадия завершилась с исключением (включая watchdog-таймаут).</summary>
	Failure,
	/// <summary>Инстанс был удалён каскадом до того, как стадия успела отработать.</summary>
	Cancelled,
}

/// <summary>
/// Полный исход одного цикла стадии для <see cref="IInstanceHandle.WaitForOutcomeAsync"/>.
/// </summary>
public sealed record StageOutcome {
	public required StageOutcomeKind Kind { get; init; }

	/// <summary>
	/// Исключение, с которым стадия завершилась (<see cref="StageOutcomeKind.Failure"/>) или
	/// причина каскадной отмены (<see cref="StageOutcomeKind.Cancelled"/>). Для
	/// <see cref="StageOutcomeKind.Success"/> — <c>null</c>.
	/// </summary>
	public Exception? Exception { get; init; }

	/// <summary>Готовый <see cref="StageOutcomeKind.Success"/>-snapshot (без аллокации).</summary>
	public static StageOutcome Success { get; } = new() { Kind = StageOutcomeKind.Success };

	public static StageOutcome FromFailure(Exception ex) =>
		new() { Kind = StageOutcomeKind.Failure, Exception = ex };

	public static StageOutcome FromCancellation(Exception ex) =>
		new() { Kind = StageOutcomeKind.Cancelled, Exception = ex };
}
