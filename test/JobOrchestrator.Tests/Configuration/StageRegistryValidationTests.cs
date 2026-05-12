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
	public void TopologicalSortReverse_LeavesFirst() {
		var reg = new StageRegistry([
			MakeStage("shops"),
			MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance)),
			MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole)),
			MakeStage("documents", new StageDependency("products", DependencyMode.Whole)),
		]);
		var stages = new[] { reg.Get("productGroups"), reg.Get("products"), reg.Get("documents") };
		var ordered = reg.TopologicalSortReverse(stages);
		// "Листья перед корнями" → documents должен быть раньше products, products раньше productGroups.
		var names = ordered.Select(s => s.Name).ToList();
		names.IndexOf("documents").Should().BeLessThan(names.IndexOf("products"));
		names.IndexOf("products").Should().BeLessThan(names.IndexOf("productGroups"));
	}
}
