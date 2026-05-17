namespace JobOrchestrator.Tests.Internal;

public sealed class DependencyResolverTests {
	private static readonly IReadOnlyDictionary<string, string> EmptyKeys =
		new Dictionary<string, string>(StringComparer.Ordinal);

	private static StageDescriptor MakeStage(string name, params StageDependency[] deps) =>
		TestStages.Make(name, new() { Dependencies = deps });

	private static Instance MakeInstance(StageDescriptor stage, Dictionary<string, string>? keys = null) {
		keys ??= new Dictionary<string, string>(StringComparer.Ordinal);
		return new Instance { Identity = new InstanceIdentity(stage, keys) };
	}

	[Fact]
	public void AreCompatible_NoSharedKeys_True() {
		var a = new Dictionary<string, string> { ["x"] = "1" };
		var b = new Dictionary<string, string> { ["y"] = "2" };
		DependencyHelpers.AreCompatible(a, b).Should().BeTrue();
	}

	[Fact]
	public void AreCompatible_SameKey_SameValue_True() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u1" };
		DependencyHelpers.AreCompatible(a, b).Should().BeTrue();
	}

	[Fact]
	public void AreCompatible_SameKey_DifferentValues_False() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u2" };
		DependencyHelpers.AreCompatible(a, b).Should().BeFalse();
	}

	[Fact]
	public void FindPairedInstance_KeyMatch_ReturnsInstance() {
		var instances = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string> { ["shops"] = "u1" });
		instances.Add(inst);

		var found = DependencyResolver.FindPairedInstance(instances, stage, new Dictionary<string, string> { ["shops"] = "u1" });
		found.Should().BeSameAs(inst);
	}

	[Fact]
	public void FindPairedInstance_KeylessParent_FoundByEmptyKeys() {
		// Безключевая родительская стадия — её единственный инстанс с пустыми DependencyKeys
		// "парный" к любому candidate, потому что projection ⊆ candidate.
		var instances = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage);
		instances.Add(inst);

		var found = DependencyResolver.FindPairedInstance(instances, stage, new Dictionary<string, string> { ["shops"] = "u1" });
		found.Should().BeSameAs(inst);
	}

	[Fact]
	public void AllDependenciesResolved_NoDeps_True() {
		var stage = MakeStage("shops");
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		DependencyResolver.AllDependenciesResolved(stage, new Dictionary<string, string>(), instances, keyspace).Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_KeyspaceMissingKey_False() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeInstance(shops);
		shopsInst.SetMetrics(shopsInst.Metrics with { LastSuccess = DateTimeOffset.UtcNow });
		instances.Add(shopsInst);
		// keyspace shops пуст — DependsOnInstance не разрешается.

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, instances, keyspace);
		resolved.Should().BeFalse();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_AllOk_True() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeInstance(shops);
		shopsInst.SetMetrics(shopsInst.Metrics with { LastSuccess = DateTimeOffset.UtcNow });
		instances.Add(shopsInst);
		keyspace.Add(new InstanceIdentity(shops, EmptyKeys), "u1");

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, instances, keyspace);
		resolved.Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_InstanceDep_ParentNeverSucceeded_StillResolves_Reactive() {
		// REACTIVE-семантика: для DependsOnInstance LastSuccess эмитера НЕ требуется. Сам факт публикации
		// ключа в keyspace = «событие состоялось», child материализуется немедленно — даже пока emitter
		// ещё внутри ExecuteAsync (long-running emitter pattern: shops эмитит постранично).
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var shopsInst = MakeInstance(shops);
		// LastSuccess = null — родитель ни разу не был успешен, но ключ уже опубликован.
		instances.Add(shopsInst);
		keyspace.Add(new InstanceIdentity(shops, EmptyKeys), "u1");

		var resolved = DependencyResolver.AllDependenciesResolved(
			pg, new Dictionary<string, string> { ["shops"] = "u1" }, instances, keyspace);
		resolved.Should().BeTrue("DependsOnInstance реактивен — child создаётся сразу при AddKey");
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_NoParentInstance_False() {
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency(x, DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		// Нет инстансов x.

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), instances, keyspace);
		resolved.Should().BeFalse();
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_ParentSucceededOnce_True() {
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency(x, DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var xInst = MakeInstance(x);
		xInst.SetMetrics(xInst.Metrics with { LastSuccess = DateTimeOffset.UtcNow });
		instances.Add(xInst);

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), instances, keyspace);
		resolved.Should().BeTrue();
	}

	[Fact]
	public void AllDependenciesResolved_WholeDep_MonotonicLastSuccess_True_AfterFailures() {
		// Инвариант монотонности LastSuccess: парный инстанс родителя имел успех хотя бы раз,
		// потом ушёл в серию неуспехов — зависимая стадия должна продолжать резолвиться.
		var x = MakeStage("x");
		var y = MakeStage("y", new StageDependency(x, DependencyMode.Whole));
		var instances = new InstanceManager();
		var keyspace = new KeyspaceRegistry();
		var xInst = MakeInstance(x);
		xInst.SetMetrics(xInst.Metrics with { LastSuccess = DateTimeOffset.UtcNow.AddMinutes(-30) });  // успех 30 минут назад
		xInst.SetMetrics(xInst.Metrics with { ConsecutiveFailures = 5 });                                // и потом 5 неуспехов
		xInst.SetMetrics(xInst.Metrics with { LastError = "boom" });
		instances.Add(xInst);

		var resolved = DependencyResolver.AllDependenciesResolved(y, new Dictionary<string, string>(), instances, keyspace);
		resolved.Should().BeTrue();  // монотонная LastSuccess != null → разрешено
	}
}
