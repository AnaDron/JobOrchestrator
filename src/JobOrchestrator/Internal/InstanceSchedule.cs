namespace JobOrchestrator.Internal;

/// <summary>
/// Расписание auto-тиков инстанса для <see cref="DueScanner"/>.
/// Часть атомарного <see cref="JobMetrics"/>-снимка.
/// </summary>
/// <param name="NextAutoUtc">
/// Время следующего Auto-тика. <c>null</c> = инстанс не запланирован (только что создан, Running —
/// перепланируется в StageCompleted/Failed-handler-е).
/// </param>
internal sealed record InstanceSchedule(DateTimeOffset? NextAutoUtc) {
	public static InstanceSchedule Unscheduled { get; } = new(NextAutoUtc: null);
}
