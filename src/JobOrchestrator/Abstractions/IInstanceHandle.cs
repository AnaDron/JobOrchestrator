namespace JobOrchestrator.Abstractions;

/// <summary>
/// Public-fasade для одного логического инстанса стадии — идентификатор + операции над ним.
/// Получается через <see cref="IStageHandle"/>-индексаторы (<c>stage[InstanceKey.None]</c> или
/// <c>stage[(name, value), ...]</c>).
/// <para>
/// Identity-based, а не Instance-based: handle остаётся валидным **до** материализации
/// runtime-инстанса (например, для late-register <see cref="WaitForSuccessAsync"/>) и **после**
/// каскадного удаления. <see cref="State"/>/<see cref="Snapshot"/> возвращают <c>null</c>, если
/// инстанс не существует на момент чтения.
/// </para>
/// </summary>
public interface IInstanceHandle {
	/// <summary>Имя стадии.</summary>
	string StageName { get; }

	/// <summary>Композитный ключ инстанса (имя зависимой стадии → значение). Пустой для безключевых.</summary>
	IReadOnlyDictionary<string, string> DependencyKeys { get; }

	/// <summary>Канонический human-readable идентификатор для логирования: <c>"stage[k1=v1,k2=v2]"</c>.</summary>
	string FullyQualifiedName { get; }

	/// <summary>
	/// Текущее состояние lifecycle инстанса, либо <c>null</c> если инстанс не существует
	/// (ещё не материализован зависимостями или удалён каскадом).
	/// </summary>
	InstanceLifecycleState? State { get; }

	/// <summary>
	/// Снимок диагностики инстанса (метрики, FQN, состояние) — либо <c>null</c> если инстанса нет.
	/// Lock-free: атомарный snapshot полей через immutable <c>JobMetrics</c>.
	/// </summary>
	InstanceInfo? Snapshot { get; }

	/// <summary>
	/// Запросить ручной запуск инстанса. Manual игнорирует retry-delay, но уважает debounce-окно.
	/// Если инстанс не существует — <see cref="TriggerResult.NotFound"/>.
	/// </summary>
	Task<TriggerResult> TriggerAsync(CancellationToken ct = default);

	/// <summary>
	/// Завершается, когда инстанс успешно отработает хотя бы один раз. Memoized: если на момент
	/// вызова <c>LastSuccess != null</c> — Task сразу завершён. Если инстанс удалён каскадом до
	/// первого успеха — <see cref="InvalidOperationException"/>.
	/// </summary>
	Task WaitForSuccessAsync(CancellationToken ct = default);

	/// <summary>
	/// Завершается на ПЕРВОМ ИСХОДЕ следующего цикла (Success/Failure/Cancelled).
	/// Caller отвечает за порядок «Register → Trigger», если хочет ждать именно текущий запуск.
	/// </summary>
	Task<StageOutcome> WaitForOutcomeAsync(CancellationToken ct = default);
}
