using System.Collections.Concurrent;

namespace JobOrchestrator.Internal;

/// <summary>
/// Owner runtime-состояния <see cref="StageInstance"/>-ов. Lookup идёт по <see cref="InstanceIdentity"/>
/// (canonical hash-equality по <c>(StageName, EncodedKey)</c>). Структуры на <see cref="ConcurrentDictionary{TKey,TValue}"/>:
/// writes — из event-loop-consumer-потока (single writer); reads — из любого потока
/// (<see cref="DueScanner"/>, <see cref="IJobOrchestrator.GetOverview"/>), без блокировок.
/// </summary>
internal sealed class InstanceManager {
	private readonly ConcurrentDictionary<InstanceIdentity, StageInstance> _instances = new();
	private readonly ConcurrentDictionary<string, ConcurrentDictionary<StageInstance, byte>> _byStage = new(StringComparer.Ordinal);
	private static readonly ConcurrentDictionary<StageInstance, byte> EmptySet = new();

	public bool Exists(string stageName, IReadOnlyDictionary<string, string> keys) =>
		Find(stageName, keys) is not null;

	public StageInstance? Find(string stageName, IReadOnlyDictionary<string, string> keys) {
		// Сначала пробуем через secondary index — избегаем создания временного InstanceIdentity для lookup.
		if (!_byStage.TryGetValue(stageName, out var set)) return null;
		var enc = DependencyKey.Encode(keys);
		foreach (var inst in set.Keys) {
			if (string.Equals(inst.Identity.EncodedKey, enc, StringComparison.Ordinal)) return inst;
		}
		return null;
	}

	public StageInstance? Find(InstanceIdentity identity) =>
		_instances.TryGetValue(identity, out var inst) ? inst : null;

	public void Add(StageInstance instance) {
		if (!_instances.TryAdd(instance.Identity, instance)) {
			throw new InvalidOperationException($"Дубль инстанса в InstanceManager: {instance.Identity.FullyQualifiedName}");
		}
		var set = _byStage.GetOrAdd(instance.Identity.Stage.Name, _ => new ConcurrentDictionary<StageInstance, byte>());
		set.TryAdd(instance, 0);
	}

	public bool Remove(StageInstance instance) {
		var removed = _instances.TryRemove(instance.Identity, out _);
		if (removed && _byStage.TryGetValue(instance.Identity.Stage.Name, out var set)) {
			set.TryRemove(instance, out _);
		}
		return removed;
	}

	/// <summary>O(1)-доступ к инстансам стадии через secondary index. Thread-safe.</summary>
	public ICollection<StageInstance> InstancesOf(string stageName) =>
		_byStage.TryGetValue(stageName, out var set) ? set.Keys : EmptySet.Keys;

	/// <summary>Все инстансы. Snapshot enumeration — безопасно итерировать одновременно с мутациями.</summary>
	public ICollection<StageInstance> All => _instances.Values;

	public int Count => _instances.Count;

	/// <summary>
	/// Снимок состояния всех инстансов. Lock-free через atomic-reads StageInstance-полей.
	/// Snapshot полей внутри одного инстанса согласован (через <see cref="JobMetrics"/>-record), но
	/// между разными инстансами snapshot eventually-consistent. Это приемлемо для диагностики.
	/// </summary>
	public InstancesOverview Snapshot() {
		var infos = new List<InstanceInfo>(_instances.Count);
		foreach (var i in _instances.Values) {
			// Один атомарный snapshot метрик — все 5 полей согласованы между собой.
			var m = i.Metrics;
			infos.Add(new InstanceInfo {
				StageName = i.Identity.Stage.Name,
				DependencyKeys = i.Identity.DependencyKeys,
				FullyQualifiedName = i.Identity.FullyQualifiedName,
				State = i.State,
				LastSuccess = m.LastSuccess,
				LastAttempt = m.LastAttempt,
				ConsecutiveFailures = m.ConsecutiveFailures,
				LastError = m.LastError,
			});
		}
		return new InstancesOverview(infos);
	}
}
