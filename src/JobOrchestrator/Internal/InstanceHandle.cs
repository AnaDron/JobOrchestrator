namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IInstanceHandle"/>. Identity-based: handle хранит <see cref="InstanceIdentity"/>,
/// State/Snapshot читают текущее состояние через <see cref="InstanceManager"/> on-demand. Handle переживает
/// как pre-materialization, так и cascade-removal инстанса.
/// <para>
/// <c>record class</c> — для value-equality по <c>(Runtime, Identity)</c>. Два handle-instance с
/// одинаковым Identity считаются равными (можно класть в <c>Dictionary&lt;IInstanceHandle, T&gt;</c>
/// или <c>HashSet</c>).
/// </para>
/// </summary>
internal sealed record class InstanceHandle(JobOrchestratorRuntime Runtime, InstanceIdentity Identity) : IInstanceHandle {
	public string StageName => Identity.Stage.Name;
	public IReadOnlyDictionary<string, string> DependencyKeys => Identity.DependencyKeys;
	public string FullyQualifiedName => Identity.FullyQualifiedName;

	public InstanceLifecycleState? State => Runtime.FindInstance(Identity)?.State;

	public InstanceInfo? Snapshot {
		get {
			var instance = Runtime.FindInstance(Identity);
			if (instance is null) return null;
			var m = instance.Metrics;
			return new InstanceInfo {
				StageName = Identity.Stage.Name,
				DependencyKeys = Identity.DependencyKeys,
				FullyQualifiedName = Identity.FullyQualifiedName,
				State = instance.State,
				LastSuccess = m.LastSuccess,
				LastAttempt = m.LastAttempt,
				ConsecutiveFailures = m.ConsecutiveFailures,
				LastError = m.LastError,
			};
		}
	}

	public Task<TriggerResult> TriggerAsync(CancellationToken ct = default) =>
		Runtime.TriggerAsync(Identity, ct);

	public Task WaitForSuccessAsync(CancellationToken ct = default) =>
		Runtime.WaitForStageSuccessAsync(Identity, ct);

	public Task<StageOutcome> WaitForOutcomeAsync(CancellationToken ct = default) =>
		Runtime.WaitForStageOutcomeAsync(Identity, ct);
}
