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
	/// Если инстанс в каскадном удалении — <see cref="TriggerResult.Terminating"/>.
	/// </summary>
	Task<TriggerResult> TriggerAsync(CancellationToken ct = default);

	/// <summary>
	/// Завершается, когда инстанс успешно отработает хотя бы один раз. Memoized: если на момент
	/// вызова <c>LastSuccess != null</c> — Task сразу завершён. Если инстанс удалён каскадом до
	/// первого успеха — <see cref="InvalidOperationException"/>.
	/// </summary>
	Task WaitForSuccessAsync(CancellationToken ct = default);

	/// <summary>
	/// Завершается на ПЕРВОМ ИСХОДЕ (Success/Failure/Cancelled), произошедшем <b>после</b> регистрации.
	/// <para>
	/// <b>Без мемоизации.</b> В отличие от <see cref="WaitForSuccessAsync"/>, метод не возвращает
	/// уже-произошедший исход: если runner завершил итерацию ДО вызова <c>WaitForOutcomeAsync</c>,
	/// этот исход потерян для caller'а — Task будет ждать <i>следующий</i> цикл. Для periodic-стадий
	/// следующий tick придёт через <c>Interval</c>; для one-shot/keyed-стадий без re-triggering Task
	/// зависнет до cancellation.
	/// </para>
	/// <para>
	/// Caller отвечает за порядок «Register → Trigger», если хочет ждать именно текущий запуск
	/// (например, <c>var t = handle.WaitForOutcomeAsync(); await handle.TriggerAsync(); var outcome = await t;</c>).
	/// </para>
	/// </summary>
	Task<StageOutcome> WaitForOutcomeAsync(CancellationToken ct = default);
}
