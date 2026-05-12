namespace JobOrchestrator.Internal;

/// <summary>
/// Единственный owner runtime-состояния инстансов. Все операции — через event-loop consumer thread (single-threaded).
/// </summary>
internal sealed class JobManager {
	private readonly Dictionary<(string Stage, string EncodedKey), Job> _jobs = new();

	public bool Exists(string stageName, IReadOnlyDictionary<string, string> keys) {
		return _jobs.ContainsKey((stageName, DependencyKey.Encode(keys)));
	}

	public Job? Find(string stageName, IReadOnlyDictionary<string, string> keys) {
		return _jobs.TryGetValue((stageName, DependencyKey.Encode(keys)), out var j) ? j : null;
	}

	public void Add(Job job) {
		var key = (job.Stage.Name, job.EncodedKey);
		if (!_jobs.TryAdd(key, job)) {
			throw new InvalidOperationException($"Дубль инстанса в JobManager: {job.FullyQualifiedName}");
		}
	}

	public bool Remove(Job job) {
		return _jobs.Remove((job.Stage.Name, job.EncodedKey));
	}

	public IEnumerable<Job> InstancesOf(string stageName) {
		foreach (var j in _jobs.Values) {
			if (string.Equals(j.Stage.Name, stageName, StringComparison.Ordinal)) {
				yield return j;
			}
		}
	}

	public IEnumerable<Job> AllJobs => _jobs.Values;

	public int Count => _jobs.Count;

	public JobOverview ToOverview() {
		List<JobInfo> infos = new(_jobs.Count);
		foreach (var j in _jobs.Values) {
			infos.Add(new JobInfo {
				StageName = j.Stage.Name,
				DependencyKeys = j.DependencyKeys,
				FullyQualifiedName = j.FullyQualifiedName,
				State = j.State,
				LastSuccess = j.LastSuccess,
				LastAttempt = j.LastAttempt,
				ConsecutiveFailures = j.ConsecutiveFailures,
				LastError = j.LastError,
			});
		}
		return new JobOverview(infos);
	}
}
