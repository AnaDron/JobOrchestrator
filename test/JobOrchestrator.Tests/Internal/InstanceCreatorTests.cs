namespace JobOrchestrator.Tests.Internal;

public sealed class InstanceCreatorTests {
	private static readonly IReadOnlyDictionary<string, string> EmptyKeys =
		new Dictionary<string, string>(StringComparer.Ordinal);

	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name, params StageDependency[] deps) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = deps,
	};

	private static Instance MakeInstanceWithSuccess(StageDescriptor stage, Dictionary<string, string>? keys = null) {
		keys ??= new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new Instance { Identity = new InstanceIdentity(stage, keys) };
		inst.SetMetrics(inst.Metrics with { LastSuccess = DateTimeOffset.UtcNow });
		return inst;
	}

	[Fact]
	public void EvaluateAndCreate_KeylessStage_CreatesOneInstanceWithEmptyKeys() {
		var stage = MakeStage("shops");
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(stage);
		created.Should().ContainSingle();
		created[0].Stage.Should().Be(stage);
		created[0].DependencyKeys.Should().BeEmpty();
		created[0].FullyQualifiedName.Should().Be("shops[]");
		instances.Exists("shops", new Dictionary<string, string>()).Should().BeTrue();
	}

	[Fact]
	public void EvaluateAndCreate_AlreadyExists_NoDuplicate() {
		var stage = MakeStage("shops");
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		creator.EvaluateAndCreate(stage).Should().ContainSingle();
		creator.EvaluateAndCreate(stage).Should().BeEmpty();  // идемпотентно
		instances.Count.Should().Be(1);
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnInstance_NoKeyspace_NoInstance() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(shops));  // shops успешен, но keyspace пуст
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		creator.EvaluateAndCreate(pg).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnInstance_KeyspaceHasKeys_CreatesPerKey() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(shops));
		keyspace.Add(new InstanceIdentity(shops, EmptyKeys), "u1");
		keyspace.Add(new InstanceIdentity(shops, EmptyKeys), "u2");
		keyspace.Add(new InstanceIdentity(shops, EmptyKeys), "u3");
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(pg);
		created.Should().HaveCount(3);
		created.Select(j => j.DependencyKeys["shops"]).Should().BeEquivalentTo("u1", "u2", "u3");
		created.Select(j => j.FullyQualifiedName).Should().BeEquivalentTo(
			"productGroups[shops=u1]",
			"productGroups[shops=u2]",
			"productGroups[shops=u3]");
	}

	[Fact]
	public void EvaluateAndCreate_DependsOn_InheritsKeys() {
		// pg[shops=u1] успешен → products[shops=u1] должен быть создан с теми же ключами.
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var products = MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u1" }));
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(products);
		created.Should().ContainSingle();
		created[0].DependencyKeys.Should().ContainKey("shops").WhoseValue.Should().Be("u1");
		created[0].FullyQualifiedName.Should().Be("products[shops=u1]");
	}

	[Fact]
	public void EvaluateAndCreate_MultipleDependsOn_FanOutPerSuccessfulInstance() {
		// 3 успешных pg → 3 products.
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var products = MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u1" }));
		instances.Add(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u2" }));
		instances.Add(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u3" }));
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(products);
		created.Select(j => j.DependencyKeys["shops"]).Should().BeEquivalentTo("u1", "u2", "u3");
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnNotYetSucceeded_NoInstance() {
		var pg = MakeStage("productGroups");
		var products = MakeStage("products", new StageDependency("productGroups", DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		// LastSuccess = null — ещё не был успешен.
		var pgInst = new Instance { Identity = new InstanceIdentity(pg, new Dictionary<string, string>(StringComparer.Ordinal)) };
		instances.Add(pgInst);
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		creator.EvaluateAndCreate(products).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_MultipleInstanceDeps_CartesianProduct() {
		// C: DependsOnInstance(A) + DependsOnInstance(B) — cartesian product (a, b).
		var a = MakeStage("a");
		var b = MakeStage("b");
		var c = MakeStage("c",
			new StageDependency("a", DependencyMode.Instance),
			new StageDependency("b", DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(a));
		instances.Add(MakeInstanceWithSuccess(b));
		keyspace.Add(new InstanceIdentity(a, EmptyKeys), "1");
		keyspace.Add(new InstanceIdentity(a, EmptyKeys), "2");
		keyspace.Add(new InstanceIdentity(b, EmptyKeys), "x");
		keyspace.Add(new InstanceIdentity(b, EmptyKeys), "y");
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(c);
		// 2 × 2 = 4 комбинации
		created.Should().HaveCount(4);
		var pairs = created.Select(j => (j.DependencyKeys["a"], j.DependencyKeys["b"])).OrderBy(p => p).ToList();
		pairs.Should().BeEquivalentTo([("1", "x"), ("1", "y"), ("2", "x"), ("2", "y")]);
	}

	[Fact]
	public void EvaluateAndCreate_MergeIncompatibleKeys_SkipsCombination() {
		// stage X имеет DependsOn(A) и DependsOn(B). А-инстанс имеет {k=1}, B-инстанс {k=2}.
		// Они несовместимы — комбинация пропускается, инстанс X не создаётся.
		var a = MakeStage("a");
		var b = MakeStage("b");
		var x = MakeStage("x",
			new StageDependency("a", DependencyMode.Whole),
			new StageDependency("b", DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(a, new Dictionary<string, string> { ["k"] = "1" }));
		instances.Add(MakeInstanceWithSuccess(b, new Dictionary<string, string> { ["k"] = "2" }));
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		creator.EvaluateAndCreate(x).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_MergeCompatibleKeys_SuccessfullyMerges() {
		// X с DependsOn(A[k=1]) и DependsOn(B[k=1]) — совместимы, merge → {k=1}.
		var a = MakeStage("a");
		var b = MakeStage("b");
		var x = MakeStage("x",
			new StageDependency("a", DependencyMode.Whole),
			new StageDependency("b", DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		instances.Add(MakeInstanceWithSuccess(a, new Dictionary<string, string> { ["k"] = "1" }));
		instances.Add(MakeInstanceWithSuccess(b, new Dictionary<string, string> { ["k"] = "1" }));
		var creator = new InstanceCreator(instances, keyspace, new StageRegistry([]), TimeProvider.System, System.Threading.Channels.Channel.CreateUnbounded<OrchestratorEvent>());

		var created = creator.EvaluateAndCreate(x);
		created.Should().ContainSingle();
		created[0].DependencyKeys.Should().ContainKey("k").WhoseValue.Should().Be("1");
	}
}
