namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IIterationHandle"/>. Тонкая обёртка: identity для FQN + готовый Task,
/// который event-loop резолвит в completion-handler-е (<c>TrySetResult</c> на success либо
/// <c>TrySetException(IterationFailedException)</c> на failure-исход любой природы).
/// </summary>
internal sealed class IterationHandle(InstanceIdentity identity, Task completion) : IIterationHandle {
	public string FullyQualifiedName => identity.FullyQualifiedName;
	public Task Completion => completion;
}
