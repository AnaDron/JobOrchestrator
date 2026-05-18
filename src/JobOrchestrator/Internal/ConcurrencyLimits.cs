using System.Collections.Frozen;

namespace JobOrchestrator.Internal;

/// <summary>
/// Per-stage семафоры для ограничения числа одновременно работающих инстансов одной стадии.
/// Создаётся в конструкторе на основе <see cref="StageDescriptor.ConcurrencyLimit"/>; стадии без лимита
/// не имеют семафора (метод <see cref="TryAcquire"/> возвращает <c>true</c> сразу).
/// <para>
/// Семантика: <see cref="TryAcquire"/> синхронно (event-loop-thread) пытается захватить токен;
/// если лимит выбран — возвращает <c>false</c>, и event loop пропускает iteration (DueScanner / Manual trigger
/// получают <see cref="TriggerResult.ConcurrencyDeferred"/>).
/// <see cref="Release"/> вызывается из <see cref="StageRunner"/> finally — гарантирует release
/// при любом исходе итерации.
/// </para>
/// <para>
/// <b>Thread-safety.</b> Хранилище — <see cref="FrozenDictionary{TKey,TValue}"/>, immutable после
/// конструктора. <see cref="TryAcquire"/> вызывается из event-loop-thread, <see cref="Release"/> —
/// из ThreadPool runner-finally; concurrent reads безопасны, мутаций словаря после старта нет.
/// </para>
/// </summary>
internal sealed class ConcurrencyLimits {
	private readonly FrozenDictionary<StageDescriptor, SemaphoreSlim> _byStage;

	public ConcurrencyLimits(StageRegistry registry) {
		ArgumentNullException.ThrowIfNull(registry);
		var seed = new Dictionary<StageDescriptor, SemaphoreSlim>();
		foreach (var stage in registry.AllStages) {
			if (stage.ConcurrencyLimit is { } limit) {
				seed[stage] = new SemaphoreSlim(limit, limit);
			}
		}
		_byStage = seed.ToFrozenDictionary();
	}

	/// <summary>
	/// <c>true</c>, если у стадии нет лимита или токен успешно захвачен. <c>false</c>, если лимит выбран —
	/// caller'у нужно отложить запуск (в случае Auto-тика — пометить инстанс как WaitingRetry с коротким
	/// re-schedule; в случае Manual — вернуть пользователю Debounced/similar).
	/// </summary>
	public bool TryAcquire(StageDescriptor stage) =>
		!_byStage.TryGetValue(stage, out var sem) || sem.Wait(0);

	/// <summary>
	/// Освобождает токен. Безопасно вызывать для стадий без лимита (no-op).
	/// </summary>
	public void Release(StageDescriptor stage) {
		if (_byStage.TryGetValue(stage, out var sem)) sem.Release();
	}
}
