namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр keyspace, привязанного к <b>конкретному инстансу-эмитеру</b> через его <see cref="InstanceIdentity"/>.
/// Bucket-key — <see cref="InstanceIdentity"/>; каждый bucket хранит ключи, опубликованные данным
/// инстансом-эмитером через <see cref="JobContext.AddKeyAsync"/> или внешний
/// <see cref="IStageHandle.RegisterKey"/>.
/// <para>
/// API принимает <see cref="InstanceIdentity"/>, и внутреннее хранилище тоже ключуется по Identity
/// (через Equals/GetHashCode по <c>(Stage.Name, EncodedKey)</c>). Это даёт консистентность с другими
/// реестрами SDK (<see cref="InstanceManager"/>, <see cref="SuccessWaiters"/>) — единый паттерн
/// «реестр инстансов ключуется по Identity».
/// </para>
/// <para>
/// Multi-instance эмитер: если стадия с <c>DependsOnInstance(parent)</c> сама эмитит ключи, оба её
/// инстанса пишут в РАЗНЫЕ bucket-ы (разные Identity). При удалении одного его bucket исчезает
/// атомарно (<see cref="RemoveInstance"/>); ключи других инстансов той же стадии не затрагиваются.
/// </para>
/// </summary>
internal sealed class KeyspaceRegistry {
	private readonly Dictionary<InstanceIdentity, EmitterBucket> _buckets = new();
	// Per-stage index: StageDescriptor → набор Identity-эмитеров, для быстрой итерации bucket-ов стадии.
	private readonly Dictionary<StageDescriptor, HashSet<InstanceIdentity>> _byStage = [];

	/// <summary>Добавляет ключ в bucket эмитера. <c>true</c>, если ключ был новый; <c>false</c> — идемпотентно.</summary>
	public bool Add(InstanceIdentity emitter, string key) {
		if (!_buckets.TryGetValue(emitter, out var bucket)) {
			bucket = new EmitterBucket(emitter, new HashSet<string>(StringComparer.Ordinal));
			_buckets[emitter] = bucket;
			if (!_byStage.TryGetValue(emitter.Stage, out var emitters)) {
				emitters = [];
				_byStage[emitter.Stage] = emitters;
			}
			emitters.Add(emitter);
		}
		return bucket.Keys.Add(key);
	}

	/// <summary>Удаляет ключ из bucket эмитера.</summary>
	public bool Remove(InstanceIdentity emitter, string key) =>
		_buckets.TryGetValue(emitter, out var bucket) && bucket.Keys.Remove(key);

	/// <summary>True, если bucket эмитера существует И содержит <paramref name="key"/>.</summary>
	public bool Contains(InstanceIdentity emitter, string key) =>
		_buckets.TryGetValue(emitter, out var bucket) && bucket.Keys.Contains(key);

	/// <summary>
	/// Удаляет bucket эмитера целиком (вызывается при cascade-удалении инстанса). Возвращает orphan-ключи
	/// для рекурсивного cascade потомков.
	/// </summary>
	public IReadOnlyCollection<string> RemoveInstance(InstanceIdentity emitter) {
		if (!_buckets.Remove(emitter, out var bucket)) return [];
		if (_byStage.TryGetValue(emitter.Stage, out var emitters)) {
			emitters.Remove(emitter);
			if (emitters.Count == 0) _byStage.Remove(emitter.Stage);
		}
		return bucket.Keys;
	}

	/// <summary>
	/// Все активные bucket-ы данной стадии — пары <c>(emitter Identity, keys)</c>. Используется
	/// <see cref="InstanceCreator"/> для построения candidate-измерений при <c>DependsOnInstance(stage)</c>:
	/// каждый bucket даёт набор ключей одного эмитера, и InstanceCreator материализует инстансы зависимой
	/// стадии с merged DependencyKeys из emitter.Identity.DependencyKeys + ключ.
	/// <para>
	/// Возвращает materialized-список (а не yield-IEnumerable): caller итерирует один раз, нет
	/// state-machine-overhead на iterator.
	/// </para>
	/// </summary>
	public IReadOnlyList<EmitterBucket> SnapshotByStage(StageDescriptor stage) {
		if (!_byStage.TryGetValue(stage, out var emitters) || emitters.Count == 0) return [];
		var result = new List<EmitterBucket>(emitters.Count);
		foreach (var emitter in emitters) {
			if (_buckets.TryGetValue(emitter, out var bucket)) result.Add(bucket);
		}
		return result;
	}

}
