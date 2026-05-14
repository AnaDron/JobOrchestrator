namespace JobOrchestrator.Internal;

/// <summary>
/// Per-stage семафоры для ограничения числа одновременно работающих инстансов одной стадии.
/// Создаётся в конструкторе на основе <see cref="StageDescriptor.ConcurrencyLimit"/>; стадии без лимита
/// не имеют семафора (метод <see cref="TryAcquire"/> возвращает <c>true</c> сразу).
/// <para>
/// Семантика: <see cref="TryAcquire"/> синхронно (event-loop-thread) пытается захватить токен;
/// если лимит выбран — возвращает <c>false</c>, и event loop пропускает iteration (DueScanner / Manual trigger
/// получают <see cref="TriggerResult.WaitingRetry"/> для Auto или эквивалент для Manual).
/// <see cref="Release"/> вызывается из <see cref="StageRunner"/> finally — гарантирует release
/// при любом исходе итерации.
/// </para>
/// </summary>
internal sealed class ConcurrencyLimits {
	private readonly Dictionary<string, SemaphoreSlim> _byStage = new(StringComparer.Ordinal);

	public ConcurrencyLimits(StageRegistry registry) {
		ArgumentNullException.ThrowIfNull(registry);
		foreach (var stage in registry.AllStages) {
			if (stage.ConcurrencyLimit is { } limit) {
				_byStage[stage.Name] = new SemaphoreSlim(limit, limit);
			}
		}
	}

	/// <summary>
	/// <c>true</c>, если у стадии нет лимита или токен успешно захвачен. <c>false</c>, если лимит выбран —
	/// caller'у нужно отложить запуск (в случае Auto-тика — пометить инстанс как WaitingRetry с коротким
	/// re-schedule; в случае Manual — вернуть пользователю Debounced/similar).
	/// </summary>
	public bool TryAcquire(StageDescriptor stage) =>
		!_byStage.TryGetValue(stage.Name, out var sem) || sem.Wait(0);

	/// <summary>
	/// Освобождает токен. Безопасно вызывать для стадий без лимита (no-op).
	/// </summary>
	public void Release(StageDescriptor stage) {
		if (_byStage.TryGetValue(stage.Name, out var sem)) sem.Release();
	}
}
