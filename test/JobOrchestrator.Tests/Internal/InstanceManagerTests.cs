namespace JobOrchestrator.Tests.Internal;

public sealed class InstanceManagerTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = [],
	};

	private static StageInstance MakeInstance(StageDescriptor stage, Dictionary<string, string> keys) {
		var fqn = DependencyKey.FormatFullyQualifiedName(stage.Name, keys);
		var encoded = DependencyKey.Encode(keys);
		return new StageInstance { Stage = stage, DependencyKeys = keys, FullyQualifiedName = fqn, EncodedKey = encoded };
	}

	[Fact]
	public void Exists_NewManager_Empty() {
		var mgr = new InstanceManager();
		mgr.Exists("a", new Dictionary<string, string>()).Should().BeFalse();
	}

	[Fact]
	public void Add_Find_RoundTrip() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		mgr.Add(inst);
		mgr.Find("shops", new Dictionary<string, string>()).Should().BeSameAs(inst);
		mgr.Exists("shops", new Dictionary<string, string>()).Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateInstance_Throws() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		mgr.Add(MakeInstance(stage, new Dictionary<string, string>()));
		Action act = () => mgr.Add(MakeInstance(stage, new Dictionary<string, string>()));
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public void Find_DifferentKeyOrder_FindsSameInstance() {
		// Канонический encoding сортирует ключи — два словаря с разным insertion order должны найти одну запись.
		var mgr = new InstanceManager();
		var stage = MakeStage("documents");
		var keys1 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		var keys2 = new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" };
		var inst = MakeInstance(stage, keys1);
		mgr.Add(inst);
		mgr.Find("documents", keys2).Should().BeSameAs(inst);
	}

	[Fact]
	public void Remove_ExistingInstance_RemovesIt() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		mgr.Add(inst);
		mgr.Remove(inst).Should().BeTrue();
		mgr.Exists("shops", new Dictionary<string, string>()).Should().BeFalse();
	}

	[Fact]
	public void InstancesOf_FiltersByStageName() {
		var mgr = new InstanceManager();
		var stageA = MakeStage("a");
		var stageB = MakeStage("b");
		mgr.Add(MakeInstance(stageA, new Dictionary<string, string> { ["k"] = "1" }));
		mgr.Add(MakeInstance(stageA, new Dictionary<string, string> { ["k"] = "2" }));
		mgr.Add(MakeInstance(stageB, new Dictionary<string, string>()));
		mgr.InstancesOf("a").Should().HaveCount(2);
		mgr.InstancesOf("b").Should().ContainSingle();
		mgr.InstancesOf("nonexistent").Should().BeEmpty();
	}

	[Fact]
	public void ToOverview_ReflectsCurrentState() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		inst.LastSuccess = DateTimeOffset.UtcNow;
		inst.ConsecutiveFailures = 2;
		inst.LastError = "boom";
		mgr.Add(inst);

		var overview = mgr.ToOverview();
		overview.Instances.Should().ContainSingle();
		var info = overview.Instances[0];
		info.StageName.Should().Be("shops");
		info.FullyQualifiedName.Should().Be("shops[]");
		info.LastSuccess.Should().NotBeNull();
		info.ConsecutiveFailures.Should().Be(2);
		info.LastError.Should().Be("boom");
	}
}
