namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IIterationHandle"/>. Тонкая обёртка: identity для FQN + готовый Task,
/// который event-loop резолвит в completion-handler-е (<c>TrySetResult</c> на success либо
/// <c>TrySetException(IterationFailedException)</c> на failure-исход любой природы).
/// <para>
/// <see cref="Instance"/>-reverse-link создаётся лениво через <see cref="InstanceHandle"/> поверх Runtime
/// (identity-based, дешёвый).
/// </para>
/// </summary>
internal sealed class IterationHandle(JobOrchestratorRuntime runtime, InstanceIdentity identity, Task completion) : IIterationHandle {
	public IInstanceHandle Instance => new InstanceHandle(runtime, identity);
	public string FullyQualifiedName => identity.FullyQualifiedName;
	public Task Completion => completion;
}
