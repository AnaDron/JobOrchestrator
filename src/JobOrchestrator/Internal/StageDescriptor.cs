namespace JobOrchestrator.Internal;

/// <summary>Финальное (immutable) описание стадии после сборки графа. Хранится в <see cref="StageRegistry"/>.</summary>
internal sealed class StageDescriptor {
	public required string Name { get; init; }
	public required Type ServiceType { get; init; }
	public required TimeSpan Interval { get; init; }
	public required RetryPolicy RetryPolicy { get; init; }
	public required TimeSpan Debounce { get; init; }
	public TimeSpan? ExecutionTimeout { get; init; }

	/// <summary>Зависимости в порядке объявления в Fluent API.</summary>
	public required IReadOnlyList<StageDependency> Dependencies { get; init; }

	/// <summary>
	/// Имена ключей, которые ожидаются в <c>DependencyKeys</c> инстансов этой стадии — равны именам её
	/// <c>DependsOnInstance</c>-зависимостей в порядке Fluent API. Pre-computed в момент сборки <see cref="StageRegistry"/>.
	/// Для безключевой стадии (нет <c>DependsOnInstance</c>) — пустой список.
	/// </summary>
	public required IReadOnlyList<string> InstanceKeyNames { get; init; }
}
