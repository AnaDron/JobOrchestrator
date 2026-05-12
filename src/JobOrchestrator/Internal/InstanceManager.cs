namespace JobOrchestrator.Internal;

/// <summary>
/// Единственный owner runtime-состояния <see cref="StageInstance"/>-ов.
/// Все операции — через event-loop consumer thread (single-threaded).
/// </summary>
internal sealed class InstanceManager {
	private readonly Dictionary<(string Stage, string EncodedKey), StageInstance> _instances = [];
	private readonly Dictionary<string, HashSet<StageInstance>> _byStage = new(StringComparer.Ordinal);
	private static readonly HashSet<StageInstance> EmptySet = [];

	public bool Exists(string stageName, IReadOnlyDictionary<string, string> keys) =>
		_instances.ContainsKey((stageName, DependencyKey.Encode(keys)));

	public StageInstance? Find(string stageName, IReadOnlyDictionary<string, string> keys) =>
		_instances.TryGetValue((stageName, DependencyKey.Encode(keys)), out var inst) ? inst : null;

	public void Add(StageInstance instance) {
		var key = (instance.Stage.Name, instance.EncodedKey);
		if (!_instances.TryAdd(key, instance)) {
			throw new InvalidOperationException($"Дубль инстанса в InstanceManager: {instance.FullyQualifiedName}");
		}
		if (!_byStage.TryGetValue(instance.Stage.Name, out var set)) {
			set = [];
			_byStage[instance.Stage.Name] = set;
		}
		set.Add(instance);
	}

	public bool Remove(StageInstance instance) {
		var removed = _instances.Remove((instance.Stage.Name, instance.EncodedKey));
		if (removed && _byStage.TryGetValue(instance.Stage.Name, out var set)) {
			set.Remove(instance);
		}
		return removed;
	}

	/// <summary>O(1)-доступ к инстансам стадии через вторичный индекс.</summary>
	public IReadOnlyCollection<StageInstance> InstancesOf(string stageName) =>
		_byStage.TryGetValue(stageName, out var set) ? set : EmptySet;

	public IEnumerable<StageInstance> All => _instances.Values;

	public int Count => _instances.Count;

	public InstancesOverview ToOverview() {
		List<InstanceInfo> infos = new(_instances.Count);
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
