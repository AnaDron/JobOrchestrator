namespace JobOrchestrator.Tests.Internal;

public sealed class DependencyResolverTests {
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

	private static Job MakeJob(StageDescriptor stage, Dictionary<string, string>? keys = null) {
		keys ??= new Dictionary<string, string>(StringComparer.Ordinal);
		return new Job {
			Stage = stage,
			DependencyKeys = keys,
			FullyQualifiedName = DependencyKey.FormatFullyQualifiedName(stage.Name, keys),
			EncodedKey = DependencyKey.Encode(keys),
		};
	}

	[Fact]
	public void AreCompatible_NoSharedKeys_True() {
		var a = new Dictionary<string, string> { ["x"] = "1" };
		var b = new Dictionary<string, string> { ["y"] = "2" };
		DependencyResolver.AreCompatible(a, b).Should().BeTrue();
	}

	[Fact]
	public void AreCompatible_SameKey_SameValue_True() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u1" };
		DependencyResolver.AreCompatible(a, b).Should().BeTrue();
	}

	[Fact]
	public void AreCompatible_SameKey_DifferentValues_False() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u2" };
		DependencyResolver.AreCompatible(a, b).Should().BeFalse();
	}

	[Fact]
	public void FindPairedInstance_KeyMatch_ReturnsInstance() {
		var jobs = new JobManager();
		var stage = MakeStage("shops");
		var inst = MakeJob(stage, new Dictionary<string, string> { ["shops"] = "u1" });
		jobs.Add(inst);

		var found = DependencyResolver.FindPairedInstance(jobs, "shops", new Dictionary<string, string> { ["shops"] = "u1" });
		found.Should().BeSameAs(inst);
	}

	[Fact]
	public void FindPairedInstance_KeylessParent_FoundByEmptyKeys() {
		// Безключевая родительская стадия — её единственный инстанс с пустыми DependencyKeys
		// "парный" к любому candidate, потому что projection ⊆ candidate.
		var jobs = new JobManager();
		var stage = MakeStage("shops");
		var inst = MakeJob(stage);
		jobs.Add(inst);

		var found = DependencyResolver.FindPairedInstance(jobs, "shops", new Dictionary<string, string> { ["shops"] = "u1" });
		found.Should().BeSameAs(inst);
	}

	[Fact]
	public void AllDependenciesResolved_NoDeps_True() {
		var stage = MakeStage("shops");
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		DependencyResolver.AllDependenciesResolved(stage, new Dictionary<string, string>(), jobs, keyspace).Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_KeyspaceMissingKey_False() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeJob(shops);
		shopsInst.LastSuccess = DateTimeOffset.UtcNow;
		jobs.Add(shopsInst);
		// keyspace shops пуст — DependsOnInstance не разрешается.

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, jobs, keyspace);
		resolved.Should().BeFalse();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_AllOk_True() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeJob(shops);
		shopsInst.LastSuccess = DateTimeOffset.UtcNow;
		jobs.Add(shopsInst);
		keyspace.Add("shops", "u1");

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, jobs, keyspace);
		resolved.Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_ParentNeverSucceeded_False() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency("shops", DependencyMode.Instance));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeJob(shops);
		// LastSuccess = null — родитель ни разу не был успешен.
		jobs.Add(shopsInst);
		keyspace.Add("shops", "u1");

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, jobs, keyspace);
		resolved.Should().BeFalse();
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_NoParentInstance_False() {
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency("x", DependencyMode.Whole));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		// Нет инстансов x.

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), jobs, keyspace);
		resolved.Should().BeFalse();
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_ParentSucceededOnce_True() {
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency("x", DependencyMode.Whole));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		var xInst = MakeJob(x);
		xInst.LastSuccess = DateTimeOffset.UtcNow;
		jobs.Add(xInst);

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), jobs, keyspace);
		resolved.Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_MonotonicLastSuccess_True_AfterFailures() {
		// Инвариант монотонности LastSuccess: парный инстанс родителя имел успех хотя бы раз,
		// потом ушёл в серию неуспехов — зависимая стадия должна продолжать резолвиться.
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency("x", DependencyMode.Whole));
		var jobs = new JobManager();
		var keyspace = new KeyspaceRegistry();
		var xInst = MakeJob(x);
		xInst.LastSuccess = DateTimeOffset.UtcNow.AddMinutes(-30);  // успех 30 минут назад
		xInst.ConsecutiveFailures = 5;                                // и потом 5 неуспехов
		xInst.LastError = "boom";
		jobs.Add(xInst);

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), jobs, keyspace);
		resolved.Should().BeTrue();  // монотонная LastSuccess != null → разрешено
	}
}
