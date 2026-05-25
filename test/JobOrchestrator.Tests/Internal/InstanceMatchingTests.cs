using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Тесты алгоритма backtracking-merge, который раньше жил в <c>InstanceCreator</c>, а теперь —
/// internal static methods в partial-секции <c>EventLoop.InstanceMatching</c>.
/// </summary>
public sealed class InstanceMatchingTests {
	private static StageDescriptor MakeStage(string name, params StageDependency[] deps) =>
		TestStages.Make(name, new() { Dependencies = deps });

	private static Instance MakeInstanceWithSuccess(StageDescriptor stage, Dictionary<string, string>? keys = null) {
		keys ??= new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new Instance { Identity = new InstanceIdentity(stage, keys) };
		inst.SetMetrics(inst.Metrics.WithLastSuccess(DateTimeOffset.UtcNow));
		return inst;
	}

	private static Channel<OrchestratorEvent> NewChannel() =>
		Channel.CreateUnbounded<OrchestratorEvent>();

	/// <summary>Минимальный <see cref="JobOrchestratorRuntime"/> для тестов matching: пустой registry,
	/// instance-store пуст, channel и логгер — заглушки. Каждая стадия в тесте добавляется в runtime
	/// через <see cref="JobOrchestratorRuntime.AddInstance"/>.</summary>
	private static JobOrchestratorRuntime NewRuntime(Channel<OrchestratorEvent>? channel = null) {
		channel ??= NewChannel();
		var registry = new StageRegistry([]);
		return new JobOrchestratorRuntime(channel, registry, NullLogger<JobOrchestratorRuntime>.Instance);
	}

	private static List<Instance> EvaluateAndCreate(StageDescriptor stage, JobOrchestratorRuntime runtime) =>
		EventLoop.EvaluateAndCreate(stage, runtime, NewChannel(), TimeProvider.System);

	/// <summary>
	/// Helper для setup-логики «инстанс <paramref name="stage"/> с такими-то <paramref name="depKeys"/>
	/// эмитит ключи <paramref name="keys"/>». Скрывает создание Instance + Add + AddEmittedKey в одной строке.
	/// </summary>
	private static void AddEmittingInstance(
		JobOrchestratorRuntime runtime,
		StageDescriptor stage,
		Dictionary<string, string>? depKeys,
		params string[] keys
	) {
		var inst = MakeInstanceWithSuccess(stage, depKeys);
		runtime.AddInstance(inst);
		foreach (var k in keys) inst.AddEmittedKey(k);
	}

	[Fact]
	public void EvaluateAndCreate_KeylessStage_CreatesOneInstanceWithEmptyKeys() {
		var stage = MakeStage("shops");
		var runtime = NewRuntime();

		var created = EvaluateAndCreate(stage, runtime);
		created.Should().ContainSingle();
		created[0].Stage.Should().Be(stage);
		created[0].Keys.Should().BeEmpty();
		created[0].FullyQualifiedName.Should().Be("shops[]");
		runtime.ExistsInstance(new InstanceIdentity(stage)).Should().BeTrue();
	}

	[Fact]
	public void EvaluateAndCreate_AlreadyExists_NoDuplicate() {
		var stage = MakeStage("shops");
		var runtime = NewRuntime();

		EvaluateAndCreate(stage, runtime).Should().ContainSingle();
		EvaluateAndCreate(stage, runtime).Should().BeEmpty();  // идемпотентно
		runtime.InstanceCount.Should().Be(1);
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnInstance_NoKeyspace_NoInstance() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var runtime = NewRuntime();
		runtime.AddInstance(MakeInstanceWithSuccess(shops));  // shops успешен, но EmittedKeys пуст

		EvaluateAndCreate(pg, runtime).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnInstance_EmittedKeys_CreatesPerKey() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var runtime = NewRuntime();
		AddEmittingInstance(runtime, shops, depKeys: null, "u1", "u2", "u3");

		var created = EvaluateAndCreate(pg, runtime);
		created.Should().HaveCount(3);
		created.Select(j => j.Keys["shops"]).Should().BeEquivalentTo("u1", "u2", "u3");
		created.Select(j => j.FullyQualifiedName).Should().BeEquivalentTo(
			"productGroups[shops=u1]",
			"productGroups[shops=u2]",
			"productGroups[shops=u3]");
	}

	[Fact]
	public void EvaluateAndCreate_DependsOn_InheritsKeys() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var products = MakeStage("products", new StageDependency(pg, DependencyMode.Whole));
		var runtime = NewRuntime();
		runtime.AddInstance(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u1" }));

		var created = EvaluateAndCreate(products, runtime);
		created.Should().ContainSingle();
		created[0].Keys.Should().ContainKey("shops").WhoseValue.Should().Be("u1");
		created[0].FullyQualifiedName.Should().Be("products[shops=u1]");
	}

	[Fact]
	public void EvaluateAndCreate_MultipleDependsOn_FanOutPerSuccessfulInstance() {
		var shops = MakeStage("shops");
		var pg = MakeStage("productGroups", new StageDependency(shops, DependencyMode.Instance));
		var products = MakeStage("products", new StageDependency(pg, DependencyMode.Whole));
		var runtime = NewRuntime();
		runtime.AddInstance(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u1" }));
		runtime.AddInstance(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u2" }));
		runtime.AddInstance(MakeInstanceWithSuccess(pg, new Dictionary<string, string> { ["shops"] = "u3" }));

		var created = EvaluateAndCreate(products, runtime);
		created.Select(j => j.Keys["shops"]).Should().BeEquivalentTo("u1", "u2", "u3");
	}

	[Fact]
	public void EvaluateAndCreate_DependsOnNotYetSucceeded_NoInstance() {
		var pg = MakeStage("productGroups");
		var products = MakeStage("products", new StageDependency(pg, DependencyMode.Whole));
		var runtime = NewRuntime();
		var pgInst = new Instance { Identity = new InstanceIdentity(pg) };
		runtime.AddInstance(pgInst);

		EvaluateAndCreate(products, runtime).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_MultipleInstanceDeps_CartesianProduct() {
		var a = MakeStage("a");
		var b = MakeStage("b");
		var c = MakeStage("c",
			new StageDependency(a, DependencyMode.Instance),
			new StageDependency(b, DependencyMode.Instance));
		var runtime = NewRuntime();
		AddEmittingInstance(runtime, a, depKeys: null, "1", "2");
		AddEmittingInstance(runtime, b, depKeys: null, "x", "y");

		var created = EvaluateAndCreate(c, runtime);
		created.Should().HaveCount(4);
		var pairs = created.Select(j => (j.Keys["a"], j.Keys["b"])).OrderBy(p => p).ToList();
		pairs.Should().BeEquivalentTo([("1", "x"), ("1", "y"), ("2", "x"), ("2", "y")]);
	}

	[Fact]
	public void EvaluateAndCreate_MergeIncompatibleKeys_SkipsCombination() {
		var a = MakeStage("a");
		var b = MakeStage("b");
		var x = MakeStage("x",
			new StageDependency(a, DependencyMode.Whole),
			new StageDependency(b, DependencyMode.Whole));
		var runtime = NewRuntime();
		runtime.AddInstance(MakeInstanceWithSuccess(a, new Dictionary<string, string> { ["k"] = "1" }));
		runtime.AddInstance(MakeInstanceWithSuccess(b, new Dictionary<string, string> { ["k"] = "2" }));

		EvaluateAndCreate(x, runtime).Should().BeEmpty();
	}

	[Fact]
	public void EvaluateAndCreate_MergeCompatibleKeys_SuccessfullyMerges() {
		var a = MakeStage("a");
		var b = MakeStage("b");
		var x = MakeStage("x",
			new StageDependency(a, DependencyMode.Whole),
			new StageDependency(b, DependencyMode.Whole));
		var runtime = NewRuntime();
		runtime.AddInstance(MakeInstanceWithSuccess(a, new Dictionary<string, string> { ["k"] = "1" }));
		runtime.AddInstance(MakeInstanceWithSuccess(b, new Dictionary<string, string> { ["k"] = "1" }));

		var created = EvaluateAndCreate(x, runtime);
		created.Should().ContainSingle();
		created[0].Keys.Should().ContainKey("k").WhoseValue.Should().Be("1");
	}
}
