namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр keyspace, привязанного к <b>конкретному инстансу-эмитеру</b> через его <see cref="InstanceIdentity"/>.
/// Bucket-key = <c>(StageName, EncodedKey)</c> из Identity; каждый bucket хранит ключи, опубликованные
/// данным инстансом-эмитером через <see cref="JobContext.AddKey"/> или внешний
/// <see cref="IJobOrchestrator.RegisterKey"/>.
/// <para>
/// API принимает <see cref="InstanceIdentity"/>, а не сырые <c>(stageName, keys)</c>: encoded-key
/// pre-computed на Identity-объекте, один раз на lifetime инстанса. Это устраняет per-call Encode.
/// </para>
/// <para>
/// Multi-instance эмитер: если стадия с <c>DependsOnInstance(parent)</c> сама эмитит ключи, оба её
/// инстанса пишут в РАЗНЫЕ bucket-ы (разные Identity → разные EncodedKey). При удалении одного
/// его bucket исчезает атомарно (<see cref="RemoveInstance"/>); ключи других инстансов той же
/// стадии не затрагиваются.
/// </para>
/// </summary>
internal sealed class KeyspaceRegistry {
	private readonly Dictionary<(string Stage, string Emitter), EmitterBucket> _buckets = new();
	// Per-stage index: stageName → набор encoded emitter-keys, для быстрой итерации bucket-ов стадии.
	private readonly Dictionary<string, HashSet<string>> _byStage = new(StringComparer.Ordinal);

	/// <summary>Добавляет ключ в bucket эмитера. <c>true</c>, если ключ был новый; <c>false</c> — идемпотентно.</summary>
	public bool Add(InstanceIdentity emitter, string key) {
		var bucketKey = (emitter.Stage.Name, emitter.EncodedKey);
		if (!_buckets.TryGetValue(bucketKey, out var bucket)) {
			bucket = new EmitterBucket(emitter, new HashSet<string>(StringComparer.Ordinal));
			_buckets[bucketKey] = bucket;
			if (!_byStage.TryGetValue(emitter.Stage.Name, out var encSet)) {
				encSet = new HashSet<string>(StringComparer.Ordinal);
				_byStage[emitter.Stage.Name] = encSet;
			}
			encSet.Add(emitter.EncodedKey);
		}
		return bucket.Keys.Add(key);
	}

	/// <summary>Удаляет ключ из bucket эмитера.</summary>
	public bool Remove(InstanceIdentity emitter, string key) =>
		_buckets.TryGetValue((emitter.Stage.Name, emitter.EncodedKey), out var bucket) && bucket.Keys.Remove(key);

	/// <summary>True, если bucket эмитера существует И содержит <paramref name="key"/>.</summary>
	public bool Contains(InstanceIdentity emitter, string key) =>
		_buckets.TryGetValue((emitter.Stage.Name, emitter.EncodedKey), out var bucket) && bucket.Keys.Contains(key);

	/// <summary>
	/// Удаляет bucket эмитера целиком (вызывается при cascade-удалении инстанса). Возвращает orphan-ключи
	/// для рекурсивного cascade потомков.
	/// </summary>
	public IReadOnlyCollection<string> RemoveInstance(InstanceIdentity emitter) {
		if (!_buckets.Remove((emitter.Stage.Name, emitter.EncodedKey), out var bucket)) return [];
		if (_byStage.TryGetValue(emitter.Stage.Name, out var encSet)) {
			encSet.Remove(emitter.EncodedKey);
			if (encSet.Count == 0) _byStage.Remove(emitter.Stage.Name);
		}
		return bucket.Keys;
	}

	/// <summary>
	/// Все активные bucket-ы данной стадии — пары <c>(emitter Identity, keys)</c>. Используется
	/// <see cref="InstanceCreator"/> для построения candidate-измерений при <c>DependsOnInstance(stage)</c>:
	/// каждый bucket даёт набор ключей одного эмитера, и InstanceCreator материализует инстансы зависимой
	/// стадии с merged DependencyKeys из emitter.Identity.DependencyKeys + ключ.
	/// </summary>
	public IEnumerable<EmitterBucket> SnapshotByStage(StageDescriptor stage) {
		if (!_byStage.TryGetValue(stage.Name, out var encSet)) yield break;
		// Копируем encSet, чтобы итерация была безопасна при мутациях того же event-loop-thread'а.
		foreach (var enc in encSet.ToArray()) {
			if (_buckets.TryGetValue((stage.Name, enc), out var bucket)) yield return bucket;
		}
	}

	/// <summary>
	/// Bucket эмитера: его <see cref="InstanceIdentity"/> (через который доступны Stage и DependencyKeys
	/// для cross-merge при создании зависимых инстансов) + Keys, которые он опубликовал.
	/// </summary>
	internal sealed record EmitterBucket(InstanceIdentity Emitter, HashSet<string> Keys);
}
