namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IInstanceHandle"/>. Identity-based: создаётся per-indexer-call,
/// держит <see cref="InstanceIdentity"/> для всех downstream-операций. State/Snapshot читают
/// текущее состояние через <see cref="InstanceManager"/> on-demand — handle переживает как
/// pre-materialization, так и cascade-removal инстанса.
/// </summary>
internal sealed class InstanceHandle(
	JobOrchestratorRuntime runtime,
	StageDescriptor stage,
	IReadOnlyDictionary<string, string> dependencyKeys
) : IInstanceHandle {
	private readonly InstanceIdentity _identity = new(stage, dependencyKeys);

	public string StageName => _identity.Stage.Name;
	public IReadOnlyDictionary<string, string> DependencyKeys => _identity.DependencyKeys;
	public string FullyQualifiedName => _identity.FullyQualifiedName;

	public InstanceLifecycleState? State => runtime.FindInstance(_identity)?.State;

	public InstanceInfo? Snapshot {
		get {
			var instance = runtime.FindInstance(_identity);
			if (instance is null) return null;
			var m = instance.Metrics;
			return new InstanceInfo {
				StageName = _identity.Stage.Name,
				DependencyKeys = _identity.DependencyKeys,
				FullyQualifiedName = _identity.FullyQualifiedName,
				State = instance.State,
				LastSuccess = m.LastSuccess,
				LastAttempt = m.LastAttempt,
				ConsecutiveFailures = m.ConsecutiveFailures,
				LastError = m.LastError,
			};
		}
	}

	public Task<TriggerResult> TriggerAsync(CancellationToken ct = default) =>
		runtime.TriggerAsync(_identity, ct);

	public Task WaitForSuccessAsync(CancellationToken ct = default) =>
		runtime.WaitForStageSuccessAsync(_identity, ct);

	public Task<StageOutcome> WaitForOutcomeAsync(CancellationToken ct = default) =>
		runtime.WaitForStageOutcomeAsync(_identity, ct);
}
