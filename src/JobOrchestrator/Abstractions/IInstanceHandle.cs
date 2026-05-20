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
	/// Запустить итерацию инстанса. Manual игнорирует retry-delay, но уважает debounce-окно.
	/// <para>
	/// Возвращает <see cref="IIterationHandle"/> — handle конкретной запущенной итерации; через
	/// <see cref="IIterationHandle.Completion"/> caller дожидается завершения. Успешный <c>await</c> =
	/// успех итерации; <see cref="IterationFailedException"/> = любой не-успешный исход (бизнес-ошибка
	/// stage handler'а / каскадная отмена / shutdown / event-loop crash) — конкретика в <see cref="IterationFailedException.Reason"/>.
	/// </para>
	/// <para>
	/// Если запуск отвергнут (инстанс не найден, в каскадном удалении, уже выполняется, попал в
	/// debounce-окно, исчерпан лимит конкуренции, оркестратор faulted) — бросает
	/// <see cref="IterationRejectedException"/> с конкретным <see cref="IterationRejectReason"/>.
	/// </para>
	/// </summary>
	Task<IIterationHandle> RunAsync(CancellationToken ct = default);

	/// <summary>
	/// Завершается, когда инстанс успешно отработает хотя бы один раз. Memoized: если на момент
	/// вызова <c>LastSuccess != null</c> — Task сразу завершён. Если инстанс удалён каскадом до
	/// первого успеха — <see cref="InvalidOperationException"/>.
	/// </summary>
	Task WaitForSuccessAsync(CancellationToken ct = default);
}
