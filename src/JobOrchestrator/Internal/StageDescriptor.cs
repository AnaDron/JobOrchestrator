namespace JobOrchestrator.Internal;

/// <summary>
/// Финальное описание стадии. Самодостаточно: содержит как raw user-заданные поля
/// (<see cref="Name"/>, <see cref="ServiceType"/>, ...), так и computed-графовые
/// (<see cref="Dependencies"/>, <see cref="ExpectedKeyNames"/>, <see cref="DependentsWhole"/>,
/// <see cref="DependentsInstance"/>, <see cref="AffectedByKeyRemoval"/>, <see cref="CancellationRank"/>).
/// <para>
/// Computed-поля имеют <c>internal set</c> и заполняются <see cref="StageRegistry"/>-ctor'ом сразу
/// после построения raw-дескриптора. Снаружи сборки они readonly — InternalsVisibleTo даёт тестам
/// возможность собирать дескрипторы в обход полного pipeline.
/// </para>
/// </summary>
internal sealed class StageDescriptor {
	// Raw — задаются пользователем через Fluent API, известны на construct-time.
	public required string Name { get; init; }
	public required Type ServiceType { get; init; }
	public required TimeSpan Interval { get; init; }
	public required RetryPolicy RetryPolicy { get; init; }
	public required TimeSpan Debounce { get; init; }
	public TimeSpan? ExecutionTimeout { get; init; }

	/// <summary>
	/// Лимит одновременно работающих инстансов стадии. <c>null</c> = без лимита (все параллельно).
	/// Используется <see cref="EventLoop"/> через <c>SemaphoreSlim</c> per stage в <c>BeginIteration</c>.
	/// </summary>
	public int? ConcurrencyLimit { get; init; }

	/// <summary>Зависимости в порядке объявления в Fluent API.</summary>
	public IReadOnlyList<StageDependency> Dependencies { get; internal set; } = null!;

	/// <summary>
	/// Имена компонентов <c>DependencyKeys</c>, ожидаемые у инстансов этой стадии. Транзитивно
	/// включает имена, унаследованные через цепочку <c>DependsOn</c>-родителей.
	/// </summary>
	public IReadOnlyList<string> ExpectedKeyNames { get; internal set; } = null!;

	/// <summary>Стадии, имеющие <c>DependsOn(this)</c> — прямые dependents.</summary>
	public IReadOnlyList<StageDescriptor> DependentsWhole { get; internal set; } = null!;

	/// <summary>Стадии, имеющие <c>DependsOnInstance(this)</c> — прямые dependents.</summary>
	public IReadOnlyList<StageDescriptor> DependentsInstance { get; internal set; } = null!;

	/// <summary>
	/// Транзитивное замыкание стадий вниз по графу: инстансы могут унаследовать компонент ключа
	/// от <c>this</c> (через прямую <c>DependsOnInstance</c> или цепочку <c>DependsOn</c>).
	/// Используется при cascade-removal по <c>KeyRemovedEvent</c>.
	/// </summary>
	public IReadOnlyList<StageDescriptor> AffectedByKeyRemoval { get; internal set; } = null!;

	/// <summary>
	/// Глобальный ранг стадии для каскадной отмены: чем меньше ранг — тем «ближе к листу»
	/// (cancel-ить первым). Pre-computed: листья (стадии без зависимых-вниз) получают ранг 0,
	/// корни — наибольший. <c>-1</c> = sentinel «не инициализирован».
	/// </summary>
	public int CancellationRank { get; internal set; } = -1;
}
