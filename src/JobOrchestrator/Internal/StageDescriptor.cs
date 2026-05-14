namespace JobOrchestrator.Internal;

/// <summary>
/// Финальное описание стадии. Самодостаточно: содержит как raw user-заданные поля
/// (<see cref="Name"/>, <see cref="ServiceType"/>, ...), так и computed-графовые
/// (<see cref="Dependencies"/>, <see cref="ExpectedKeyNames"/>, <see cref="DependentsWhole"/>,
/// <see cref="DependentsInstance"/>, <see cref="AffectedByKeyRemoval"/>, <see cref="CancellationRank"/>).
/// <para>
/// <b>Two-phase initialization:</b>
/// </para>
/// <list type="number">
/// <item>Construction: задаются только raw user-fields через <c>required init</c>. Computed-поля
/// дефолтятся как <c>null!</c> (для collection-типов) и <c>-1</c> для <see cref="CancellationRank"/>.</item>
/// <item><see cref="Initialize"/>: заполняет computed-поля через <see cref="IStageInitializer"/>.
/// Вызывается ровно один раз — из <c>JobOrchestratorBuilder.BuildRegistry</c>. Type-level барьер:
/// получить <see cref="IStageInitializer"/> можно только внутри pipeline, поэтому случайный
/// mutation практически невозможен.</item>
/// </list>
/// <para>
/// <b>Fail-fast на uninit-чтении:</b> collection-поля имеют <c>null!</c>-defaults; production-чтение
/// без Initialize даст <see cref="NullReferenceException"/>. <see cref="CancellationRank"/>
/// дефолтится в <c>-1</c> — это sentinel «не инициализирован» (валидные ранги — неотрицательные).
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

	/// <summary>Зависимости в порядке объявления в Fluent API. Заполняется через <see cref="Initialize"/>.</summary>
	public IReadOnlyList<StageDependency> Dependencies { get; private set; } = null!;

	/// <summary>
	/// Имена компонентов <c>DependencyKeys</c>, ожидаемые у инстансов этой стадии. Транзитивно
	/// включает имена, унаследованные через цепочку <c>DependsOn</c>-родителей. Заполняется через
	/// <see cref="Initialize"/>.
	/// </summary>
	public IReadOnlyList<string> ExpectedKeyNames { get; private set; } = null!;

	/// <summary>Стадии, имеющие <c>DependsOn(this)</c> — прямые dependents. Заполняется через <see cref="Initialize"/>.</summary>
	public IReadOnlyList<StageDescriptor> DependentsWhole { get; private set; } = null!;

	/// <summary>Стадии, имеющие <c>DependsOnInstance(this)</c> — прямые dependents. Заполняется через <see cref="Initialize"/>.</summary>
	public IReadOnlyList<StageDescriptor> DependentsInstance { get; private set; } = null!;

	/// <summary>
	/// Транзитивное замыкание стадий вниз по графу: инстансы могут унаследовать компонент ключа
	/// от <c>this</c> (через прямую <c>DependsOnInstance</c> или цепочку <c>DependsOn</c>).
	/// Используется при cascade-removal по <c>KeyRemovedEvent</c>. Заполняется через <see cref="Initialize"/>.
	/// </summary>
	public IReadOnlyList<StageDescriptor> AffectedByKeyRemoval { get; private set; } = null!;

	/// <summary>
	/// Глобальный ранг стадии для каскадной отмены: чем меньше ранг — тем «ближе к листу»
	/// (cancel-ить первым). Pre-computed: листья (стадии без зависимых-вниз) получают ранг 0,
	/// корни — наибольший. <c>-1</c> = sentinel «не инициализирован» (Initialize не вызван).
	/// </summary>
	public int CancellationRank { get; private set; } = -1;

	/// <summary>
	/// Заполняет computed-поля через <see cref="IStageInitializer"/>. Вызывается ровно один раз
	/// — из pipeline <c>JobOrchestratorBuilder.BuildRegistry</c>. Type-level барьер: получить
	/// IStageInitializer можно только внутри pipeline (production) или через test-helper.
	/// </summary>
	internal void Initialize(IStageInitializer initializer) {
		ArgumentNullException.ThrowIfNull(initializer);
		Dependencies = initializer.DependenciesOf(this);
		ExpectedKeyNames = initializer.ExpectedKeyNamesOf(this);
		DependentsWhole = initializer.DependentsWholeOf(this);
		DependentsInstance = initializer.DependentsInstanceOf(this);
		AffectedByKeyRemoval = initializer.AffectedByKeyRemovalOf(this);
		CancellationRank = initializer.CancellationRankOf(this);
	}
}
