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

	public InstanceInfo? Snapshot => Runtime.FindInstance(Identity)?.ToInstanceInfo();

	public Task<IIterationHandle> RunAsync(CancellationToken ct = default) =>
		Runtime.RunAsync(Identity, ct);

	public Task WaitForSuccessAsync(CancellationToken ct = default) =>
		Runtime.WaitForStageSuccessAsync(Identity, ct);
}
