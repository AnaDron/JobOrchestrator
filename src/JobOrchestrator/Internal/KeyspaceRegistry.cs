namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр keyspace, привязанного к <b>конкретному инстансу-эмитеру</b>, а не к стадии в целом.
/// Bucket-key = <c>(StageName, EncodedEmitterKeys)</c>; каждый bucket хранит ключи, опубликованные
/// данным инстансом-эмитером через <see cref="JobContext.AddKey"/> или внешний <see cref="IJobOrchestrator.RegisterKey"/>.
/// <para>
/// Это позволяет корректно поддержать multi-instance-эмитеров: если стадия с <c>DependsOnInstance(parent)</c>
/// сама эмитит ключи, оба её инстанса пишут в РАЗНЫЕ bucket-ы. При удалении одного инстанса его bucket
/// исчезает атомарно (<see cref="RemoveInstance"/>); ключи других инстансов той же стадии не затрагиваются.
/// </para>
/// <para>
/// Все мутации идут из event-loop-consumer-потока (single writer) — блокировки не нужны.
/// </para>
/// </summary>
internal sealed class KeyspaceRegistry {
	private readonly Dictionary<(string Stage, string Emitter), EmitterBucket> _buckets = new();
	// Per-stage index: stageName → набор encoded emitter-keys, чтобы быстро итерировать все bucket-ы
	// стадии без сканирования всего dictionary.
	private readonly Dictionary<string, HashSet<string>> _byStage = new(StringComparer.Ordinal);

	/// <summary>
	/// Добавляет ключ в bucket эмитера. Возвращает <c>true</c>, если ключ был новый;
	/// <c>false</c> — если уже присутствовал (идемпотентно).
	/// </summary>
	public bool Add(string stageName, IReadOnlyDictionary<string, string> emitterKeys, string key) {
		var enc = DependencyKey.Encode(emitterKeys);
		var bucketKey = (stageName, enc);
		if (!_buckets.TryGetValue(bucketKey, out var bucket)) {
			bucket = new EmitterBucket(emitterKeys, new HashSet<string>(StringComparer.Ordinal));
			_buckets[bucketKey] = bucket;
			if (!_byStage.TryGetValue(stageName, out var encSet)) {
				encSet = new HashSet<string>(StringComparer.Ordinal);
				_byStage[stageName] = encSet;
			}
			encSet.Add(enc);
		}
		return bucket.Keys.Add(key);
	}

	/// <summary>
	/// Удаляет ключ из bucket эмитера. Возвращает <c>true</c>, если ключ был удалён;
	/// <c>false</c> — если отсутствовал или bucket не существовал.
	/// </summary>
	public bool Remove(string stageName, IReadOnlyDictionary<string, string> emitterKeys, string key) {
		var enc = DependencyKey.Encode(emitterKeys);
		return _buckets.TryGetValue((stageName, enc), out var bucket) && bucket.Keys.Remove(key);
	}

	/// <summary>
	/// <c>true</c>, если bucket <c>(stageName, emitterKeys)</c> существует И содержит <paramref name="key"/>.
	/// </summary>
	public bool Contains(string stageName, IReadOnlyDictionary<string, string> emitterKeys, string key) {
		var enc = DependencyKey.Encode(emitterKeys);
		return _buckets.TryGetValue((stageName, enc), out var bucket) && bucket.Keys.Contains(key);
	}

	/// <summary>
	/// Удаляет bucket эмитера целиком (вызывается, когда сам эмитер каскадно удалён).
	/// Возвращает orphan-ключи, которые были в этом bucket — caller использует их для рекурсивной
	/// каскадной отмены потомков.
	/// </summary>
	public IReadOnlyCollection<string> RemoveInstance(string stageName, IReadOnlyDictionary<string, string> emitterKeys) {
		var enc = DependencyKey.Encode(emitterKeys);
		if (!_buckets.Remove((stageName, enc), out var bucket)) return [];
		if (_byStage.TryGetValue(stageName, out var encSet)) {
			encSet.Remove(enc);
			if (encSet.Count == 0) _byStage.Remove(stageName);
		}
		return bucket.Keys;
	}

	/// <summary>
	/// Все активные bucket-ы данной стадии — пары <c>(emitterKeys, keys)</c>. Используется
	/// <see cref="InstanceCreator"/> для построения candidate-измерений при <c>DependsOnInstance(stage)</c>:
	/// каждый bucket даёт набор ключей одного эмитера, и InstanceCreator материализует инстансы зависимой
	/// стадии с merged DependencyKeys из emitterKeys + ключ.
	/// </summary>
	public IEnumerable<EmitterBucket> SnapshotByStage(string stageName) {
		if (!_byStage.TryGetValue(stageName, out var encSet)) yield break;
		// Копируем encSet, чтобы итерация была безопасна при возможных мутациях того же event-loop'а
		// (например, обработчик ниже по стеку каскадно удалит bucket).
		foreach (var enc in encSet.ToArray()) {
			if (_buckets.TryGetValue((stageName, enc), out var bucket)) yield return bucket;
		}
	}

	/// <summary>
	/// Bucket эмитера = его DependencyKeys (для cross-merge при создании зависимых инстансов) + Keys,
	/// которые он опубликовал.
	/// </summary>
	internal sealed record EmitterBucket(IReadOnlyDictionary<string, string> EmitterKeys, HashSet<string> Keys);
}
