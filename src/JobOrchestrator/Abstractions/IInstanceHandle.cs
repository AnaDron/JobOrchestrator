namespace JobOrchestrator.Abstractions;

/// <summary>
/// Public-fasade для одного логического инстанса стадии — идентификатор + операции над ним.
/// Получается через <see cref="IStageHandle.this[InstanceKeys]"/>.
/// <para>
/// Identity-based, а не Instance-based: handle остаётся валидным **до** материализации
/// runtime-инстанса (например, для late-register <see cref="WaitForSuccessAsync"/>) и **после**
/// каскадного удаления. <see cref="State"/>/<see cref="Snapshot"/> возвращают <c>null</c>, если
/// инстанс не существует на момент чтения.
/// </para>
/// <para>
/// <b>Поток итераций:</b> <see cref="IAsyncEnumerable{IIterationHandle}"/> — <c>await foreach (var iter in handle)</c>
/// получает каждую запущенную итерацию (manual или auto-tick). Stream завершается при cascade-removal
/// этого инстанса либо при shutdown оркестратора.
/// </para>
/// </summary>
public interface IInstanceHandle : IAsyncEnumerable<IIterationHandle> {
	/// <summary>Reverse-link на стадию, к которой относится инстанс.</summary>
	IStageHandle Stage { get; }

	/// <summary>Композитный ключ инстанса. Пустой (<see cref="InstanceKeys.Empty"/>) — для безключевых.</summary>
	InstanceKeys Keys { get; }

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
	/// Активная итерация — выполняется прямо сейчас (<see cref="InstanceLifecycleState.Running"/>), либо
	/// <c>null</c> когда инстанс idle, terminating или не материализован. Соответствует <c>Instance.IsRunning</c>.
	/// </summary>
	IIterationHandle? RunningIteration { get; }

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
