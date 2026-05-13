using System.Collections.Concurrent;

namespace JobOrchestrator.Internal;

/// <summary>
/// Owner runtime-состояния <see cref="StageInstance"/>-ов. Структуры на <see cref="ConcurrentDictionary{TKey,TValue}"/> —
/// мутации идут из event-loop-consumer-потока (single writer), reads возможны из любого потока
/// (<see cref="DueScanner"/>, <see cref="IJobOrchestrator.GetOverview"/>) — без блокировок.
/// </summary>
internal sealed class InstanceManager {
	private readonly ConcurrentDictionary<(string Stage, string EncodedKey), StageInstance> _instances = new();
	private readonly ConcurrentDictionary<string, ConcurrentDictionary<StageInstance, byte>> _byStage = new(StringComparer.Ordinal);
	private static readonly ConcurrentDictionary<StageInstance, byte> EmptySet = new();

	public bool Exists(string stageName, IReadOnlyDictionary<string, string> keys) =>
		_instances.ContainsKey((stageName, DependencyKey.Encode(keys)));

	public StageInstance? Find(string stageName, IReadOnlyDictionary<string, string> keys) =>
		_instances.TryGetValue((stageName, DependencyKey.Encode(keys)), out var inst) ? inst : null;

	public void Add(StageInstance instance) {
		var key = (instance.Stage.Name, instance.EncodedKey);
		if (!_instances.TryAdd(key, instance)) {
			throw new InvalidOperationException($"Дубль инстанса в InstanceManager: {instance.FullyQualifiedName}");
		}
		var set = _byStage.GetOrAdd(instance.Stage.Name, _ => new ConcurrentDictionary<StageInstance, byte>());
		set.TryAdd(instance, 0);
	}

	public bool Remove(StageInstance instance) {
		var removed = _instances.TryRemove((instance.Stage.Name, instance.EncodedKey), out _);
		if (removed && _byStage.TryGetValue(instance.Stage.Name, out var set)) {
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
	/// Snapshot может быть eventually consistent между разными полями одного инстанса (race с writer-обновлениями),
	/// но это приемлемо для диагностики через <see cref="IJobOrchestrator.GetOverview"/>.
	/// </summary>
	public InstancesOverview Snapshot() {
		var infos = new List<InstanceInfo>(_instances.Count);
		foreach (var i in _instances.Values) {
			infos.Add(new InstanceInfo {
				StageName = i.Stage.Name,
				DependencyKeys = i.DependencyKeys,
				FullyQualifiedName = i.FullyQualifiedName,
				State = i.State,
				LastSuccess = i.LastSuccess,
				LastAttempt = i.LastAttempt,
				ConsecutiveFailures = i.ConsecutiveFailures,
				LastError = i.LastError,
			});
		}
		return new InstancesOverview(infos);
	}
}
