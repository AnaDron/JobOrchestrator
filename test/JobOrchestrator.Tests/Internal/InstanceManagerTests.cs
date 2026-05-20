namespace JobOrchestrator.Tests.Internal;

public sealed class InstanceManagerTests {
	private static StageDescriptor MakeStage(string name) => TestStages.Make(name);

	private static Instance MakeInstance(StageDescriptor stage, Dictionary<string, string> keys) =>
		new() { Identity = new InstanceIdentity(stage, keys) };

	private static InstanceIdentity Id(StageDescriptor stage, Dictionary<string, string>? keys = null) =>
		new(stage, keys ?? new Dictionary<string, string>(StringComparer.Ordinal));

	[Fact]
	public void Exists_NewManager_Empty() {
		var mgr = new InstanceManager();
		var stage = MakeStage("a");
		mgr.Exists(Id(stage)).Should().BeFalse();
	}

	[Fact]
	public void Add_Find_RoundTrip() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		mgr.Add(inst);
		mgr.Find(Id(stage)).Should().BeSameAs(inst);
		mgr.Exists(Id(stage)).Should().BeTrue();
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
		// Identity equality по (StageName, EncodedKey) — два словаря с разным insertion order должны найти одну запись.
		var mgr = new InstanceManager();
		var stage = MakeStage("documents");
		var keys1 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		var keys2 = new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" };
		var inst = MakeInstance(stage, keys1);
		mgr.Add(inst);
		mgr.Find(Id(stage, keys2)).Should().BeSameAs(inst);
	}

	[Fact]
	public void Remove_ExistingInstance_RemovesIt() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		mgr.Add(inst);
		mgr.Remove(inst).Should().BeTrue();
		mgr.Exists(Id(stage)).Should().BeFalse();
	}

	[Fact]
	public void InstancesOf_FiltersByStage() {
		var mgr = new InstanceManager();
		var stageA = MakeStage("a");
		var stageB = MakeStage("b");
		var nonexistent = MakeStage("nonexistent");
		mgr.Add(MakeInstance(stageA, new Dictionary<string, string> { ["k"] = "1" }));
		mgr.Add(MakeInstance(stageA, new Dictionary<string, string> { ["k"] = "2" }));
		mgr.Add(MakeInstance(stageB, new Dictionary<string, string>()));
		mgr.InstancesOf(stageA).Should().HaveCount(2);
		mgr.InstancesOf(stageB).Should().ContainSingle();
		mgr.InstancesOf(nonexistent).Should().BeEmpty();
	}

	[Fact]
	public void Snapshot_ReflectsCurrentState() {
		var mgr = new InstanceManager();
		var stage = MakeStage("shops");
		var inst = MakeInstance(stage, new Dictionary<string, string>());
		inst.SetMetrics(inst.Metrics.WithLastSuccess(DateTimeOffset.UtcNow));
		inst.SetMetrics(inst.Metrics.WithConsecutiveFailures(2));
		inst.SetMetrics(inst.Metrics.WithLastError("boom"));
		mgr.Add(inst);

		var overview = mgr.Snapshot();
		overview.Instances.Should().ContainSingle();
		var info = overview.Instances[0];
		info.StageName.Should().Be("shops");
		info.FullyQualifiedName.Should().Be("shops[]");
		info.LastSuccess.Should().NotBeNull();
		info.ConsecutiveFailures.Should().Be(2);
		info.LastError.Should().Be("boom");
	}
}
