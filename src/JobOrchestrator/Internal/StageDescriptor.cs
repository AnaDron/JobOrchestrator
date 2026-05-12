namespace JobOrchestrator.Internal;

/// <summary>Финальное (immutable) описание стадии после сборки графа. Хранится в <see cref="StageRegistry"/>.</summary>
internal sealed class StageDescriptor {
	public required string Name { get; init; }
	public required Type ServiceType { get; init; }
	public required TimeSpan Interval { get; init; }
	public required RetryPolicy RetryPolicy { get; init; }
	public required TimeSpan Debounce { get; init; }
	public TimeSpan? ExecutionTimeout { get; init; }

	/// <summary>Зависимости в порядке объявления в Fluent API. Используется для форматирования <c>FullyQualifiedName</c> и порядка structured-полей в logger.BeginScope.</summary>
	public required IReadOnlyList<StageDependency> Dependencies { get; init; }
}
