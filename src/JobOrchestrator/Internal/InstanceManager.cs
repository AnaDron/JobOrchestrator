namespace JobOrchestrator.Internal;

/// <summary>
/// Единственный owner runtime-состояния <see cref="StageInstance"/>-ов.
/// Все операции — через event-loop consumer thread (single-threaded).
/// </summary>
internal sealed class InstanceManager {
	private readonly Dictionary<(string Stage, string EncodedKey), StageInstance> _instances = new();

	public bool Exists(string stageName, IReadOnlyDictionary<string, string> keys) {
		return _instances.ContainsKey((stageName, DependencyKey.Encode(keys)));
	}

	public StageInstance? Find(string stageName, IReadOnlyDictionary<string, string> keys) {
		return _instances.TryGetValue((stageName, DependencyKey.Encode(keys)), out var inst) ? inst : null;
	}

	public void Add(StageInstance instance) {
		var key = (instance.Stage.Name, instance.EncodedKey);
		if (!_instances.TryAdd(key, instance)) {
			throw new InvalidOperationException($"Дубль инстанса в InstanceManager: {instance.FullyQualifiedName}");
		}
	}

	public bool Remove(StageInstance instance) {
		return _instances.Remove((instance.Stage.Name, instance.EncodedKey));
	}

	public IEnumerable<StageInstance> InstancesOf(string stageName) =>
		_instances.Values.Where(i => string.Equals(i.Stage.Name, stageName, StringComparison.Ordinal));

	public IEnumerable<StageInstance> All => _instances.Values;

	public int Count => _instances.Count;

	public JobOverview ToOverview() {
		var infos = _instances.Values.Select(i => new JobInfo {
			StageName = i.Stage.Name,
			DependencyKeys = i.DependencyKeys,
			FullyQualifiedName = i.FullyQualifiedName,
			State = i.State,
			LastSuccess = i.LastSuccess,
			LastAttempt = i.LastAttempt,
			ConsecutiveFailures = i.ConsecutiveFailures,
			LastError = i.LastError,
		}).ToList();
		return new JobOverview(infos);
	}
}
