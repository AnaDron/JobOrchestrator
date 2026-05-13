namespace JobOrchestrator.Tests.Configuration;

public sealed class StageRegistryValidationTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name, params StageDependency[] deps) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		ExecutionTimeout = null,
		Dependencies = deps,
	};

	[Fact]
	public void EmptyGraph_BuildsSuccessfully() {
		var reg = new StageRegistry([]);
		reg.AllStages.Should().BeEmpty();
	}

	[Fact]
	public void SingleStage_BuildsSuccessfully() {
		var reg = new StageRegistry([MakeStage("a")]);
		reg.AllStages.Should().HaveCount(1);
		reg.Get("a").Name.Should().Be("a");
	}

	[Fact]
	public void DuplicateStageNames_Throws() {
		Action act = () => new StageRegistry([MakeStage("a"), MakeStage("a")]);
		act.Should().Throw<JobConfigurationException>().WithMessage("*Дубль*");
	}

	[Fact]
	public void DanglingDependency_Throws() {
		Action act = () => new StageRegistry([
			MakeStage("a", new StageDependency("nonexistent", DependencyMode.Whole))
		]);
		act.Should().Throw<JobConfigurationException>().WithMessage("*nonexistent*");
	}

	[Fact]
	public void SelfCycle_Throws() {
		Action act = () => new StageRegistry([
			MakeStage("a", new StageDependency("a", DependencyMode.Whole))
		]);
		act.Should().Throw<JobConfigurationException>().WithMessage("*Цикл*");
	}

	[Fact]
	public void TwoStageCycle_Throws() {
		Action act = () => new StageRegistry([
			MakeStage("a", new StageDependency("b", DependencyMode.Whole)),
			MakeStage("b", new StageDependency("a", DependencyMode.Whole)),
		]);
		act.Should().Throw<JobConfigurationException>().WithMessage("*Цикл*");
	}

	[Fact]
	public void ThreeStageCycle_Throws() {
		Action act = () => new StageRegistry([
			MakeStage("a", new StageDependency("b", DependencyMode.Whole)),
			MakeStage("b", new StageDependency("c", DependencyMode.Whole)),
			MakeStage("c", new StageDependency("a", DependencyMode.Whole)),
		]);
		act.Should().Throw<JobConfigurationException>();
	}

	[Fact]
	public void LinearChain_BuildsSuccessfully() {
		var reg = new StageRegistry([
			MakeStage("a"),
			MakeStage("b", new StageDependency("a", DependencyMode.Whole)),
			MakeStage("c", new StageDependency("b", DependencyMode.Whole)),
		]);
		reg.AllStages.Should().HaveCount(3);
	}

	[Fact]
	public void DependsOnInstance_DetectedSeparately() {
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("products", new StageDependency("shops", DependencyMode.Instance)),
		]);
		reg.StagesDependingOnInstance("shops").Should().ContainSingle(s => s.Name == "products");
		reg.StagesDependingOn("shops").Should().BeEmpty();
	}

	[Fact]
	public void StagesAffectedByKeyRemoval_TransitiveClosure() {
		// shops → productGroups → products → documents (transitive, разные modes)
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance)),
			MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole)),
			MakeStage("documents", new StageDependency("products", DependencyMode.Whole)),
		]);
		var affected = reg.StagesAffectedByKeyRemoval("shops");
		affected.Select(s => s.Name).Should().BeEquivalentTo(["productGroups", "products", "documents"]);
	}

	[Fact]
	public void ExpectedKeyNames_TransitivelyInheritsThroughDependsOn() {
		// Цепь: shops --DependsOnInstance--> productGroups --DependsOn--> products --DependsOn--> documents.
		// Direct DependsOnInstance только у productGroups. Через цепочку DependsOn измерение `shops`
		// наследуется до documents — ExpectedKeyNames должен это отразить.
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance)),
			MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole)),
			MakeStage("documents", new StageDependency("products", DependencyMode.Whole)),
		]);

		reg.ExpectedKeyNames("shops").Should().BeEmpty();
		reg.ExpectedKeyNames("productGroups").Should().BeEquivalentTo(["shops"]);
		reg.ExpectedKeyNames("products").Should().BeEquivalentTo(["shops"], "products унаследовал измерение от productGroups через DependsOn");
		reg.ExpectedKeyNames("documents").Should().BeEquivalentTo(["shops"], "documents унаследовал то же измерение через цепочку DependsOn");
	}

	[Fact]
	public void ExpectedKeyNames_MultipleDependsOnInstance_MergesInDeclarationOrder() {
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("currencies"),
			MakeStage("rates",
				new StageDependency("shops", DependencyMode.Instance),
				new StageDependency("currencies", DependencyMode.Instance)),
		]);
		// Имена в порядке объявления: shops, потом currencies — что соответствует Fluent API
		// .DependsOnInstance(shops).DependsOnInstance(currencies).
		reg.ExpectedKeyNames("rates").Should().Equal("shops", "currencies");
	}

	[Fact]
	public void CancellationRank_LeavesAreLowest() {
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance)),
			MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole)),
			MakeStage("documents", new StageDependency("products", DependencyMode.Whole)),
		]);
		// documents — лист (никто не зависит), shops — корень.
		// Сортируя по CancellationRank возрастанию: листья первыми (idx → cancel order).
		var rankDocs = reg.CancellationRank("documents");
		var rankProducts = reg.CancellationRank("products");
		var rankPg = reg.CancellationRank("productGroups");
		var rankShops = reg.CancellationRank("shops");
		rankDocs.Should().BeLessThan(rankProducts);
		rankProducts.Should().BeLessThan(rankPg);
		rankPg.Should().BeLessThan(rankShops);
	}
}
