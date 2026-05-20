namespace JobOrchestrator.Internal;

/// <summary>
/// Immutable snapshot runtime-состояния инстанса. Заменяется атомарно через
/// <see cref="Interlocked.Exchange{T}(ref T, T)"/>. Read-side получает согласованную пару
/// <see cref="Stats"/> + <see cref="Schedule"/> из одной «эпохи» writer'а.
/// </summary>
internal sealed record JobMetrics(InstanceExecutionStats Stats, InstanceSchedule Schedule) {
	public static JobMetrics Empty { get; } = new(InstanceExecutionStats.Empty, InstanceSchedule.Unscheduled);

	/// <summary>
	/// Точечное обновление <c>NextAutoUtc</c> — единственный production-writer на горячем пути
	/// (<c>BeginIteration</c>, <c>DeferIteration</c>, <c>InstanceCreator.MaterializeInstance</c>).
	/// Read-side доступ к scheduling-полю — через <c>metrics.Schedule.NextAutoUtc</c>.
	/// Stats-helpers (<c>WithLastSuccess</c> и др.) живут в test-проекте — production-writers
	/// обновляют несколько полей сразу через единый <c>new JobMetrics(...)</c> либо <c>with</c>-блоки.
	/// </summary>
	public JobMetrics WithNextAutoUtc(DateTimeOffset? nextAutoUtc) =>
		this with { Schedule = Schedule with { NextAutoUtc = nextAutoUtc } };
}
