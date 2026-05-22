using System.Runtime.CompilerServices;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IInstanceHandle"/>. Identity-based: handle хранит <see cref="InstanceIdentity"/>,
/// State/Snapshot/RunningIteration читают текущее состояние через <see cref="JobOrchestratorRuntime"/> on-demand.
/// Handle переживает как pre-materialization, так и cascade-removal инстанса.
/// <para>
/// <c>record class</c> — для value-equality по <c>(Runtime, Identity)</c>. Два handle-instance с
/// одинаковым Identity считаются равными.
/// </para>
/// </summary>
internal sealed record class InstanceHandle(JobOrchestratorRuntime Runtime, InstanceIdentity Identity) : IInstanceHandle {
	public IStageHandle Stage => Runtime.GetStageHandle(Identity.Stage);
	public InstanceKeys Keys => Identity.Keys;
	public string FullyQualifiedName => Identity.FullyQualifiedName;

	public InstanceLifecycleState? State => Runtime.FindInstance(Identity)?.State;

	public InstanceInfo? Snapshot => Runtime.FindInstance(Identity)?.ToInstanceInfo();

	public IIterationHandle? RunningIteration => Runtime.FindInstance(Identity)?.RunningIteration;

	public Task<IIterationHandle> RunAsync(CancellationToken ct = default) =>
		Runtime.RunAsync(Identity, ct);

	public Task WaitForSuccessAsync(CancellationToken ct = default) =>
		Runtime.WaitForStageSuccessAsync(Identity, ct);

	/// <summary>
	/// Stream итераций инстанса. Подписка возможна только когда инстанс материализован — иначе stream
	/// сразу завершается (yield break). Stream также завершается при cascade-removal этого инстанса.
	/// </summary>
	public async IAsyncEnumerator<IIterationHandle> GetAsyncEnumerator(CancellationToken ct = default) {
		var instance = Runtime.FindInstance(Identity);
		if (instance is null) yield break;

		await using var subscriber = instance.SubscribeIterations();
		await foreach (var iter in subscriber.Reader.ReadAllAsync(ct).ConfigureAwait(false)) {
			yield return iter;
		}
	}
}
