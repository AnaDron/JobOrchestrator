namespace JobOrchestrator.Tests.Internal;

public sealed class JobManagerTests {
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

	private static Job MakeJob(StageDescriptor stage, Dictionary<string, string> keys) {
		var fqn = DependencyKey.FormatFullyQualifiedName(stage.Name, keys);
		var encoded = DependencyKey.Encode(keys);
		return new Job { Stage = stage, DependencyKeys = keys, FullyQualifiedName = fqn, EncodedKey = encoded };
	}

	[Fact]
	public void Exists_NewManager_Empty() {
		var mgr = new JobManager();
		mgr.Exists("a", new Dictionary<string, string>()).Should().BeFalse();
	}

	[Fact]
	public void Add_Find_RoundTrip() {
		var mgr = new JobManager();
		var stage = MakeStage("shops");
		var job = MakeJob(stage, new Dictionary<string, string>());
		mgr.Add(job);
		mgr.Find("shops", new Dictionary<string, string>()).Should().BeSameAs(job);
		mgr.Exists("shops", new Dictionary<string, string>()).Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateInstance_Throws() {
		var mgr = new JobManager();
		var stage = MakeStage("shops");
		mgr.Add(MakeJob(stage, new Dictionary<string, string>()));
		Action act = () => mgr.Add(MakeJob(stage, new Dictionary<string, string>()));
		act.Should().Throw<InvalidOperationException>();
	}

	[Fact]
	public void Find_DifferentKeyOrder_FindsSameInstance() {
		// Канонический encoding сортирует ключи — два словаря с разным insertion order должны найти одну запись.
		var mgr = new JobManager();
		var stage = MakeStage("documents");
		var keys1 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		var keys2 = new Dictionary<string, string> { ["b"] = "2", ["a"] = "1" };
		var job = MakeJob(stage, keys1);
		mgr.Add(job);
		mgr.Find("documents", keys2).Should().BeSameAs(job);
	}

	[Fact]
	public void Remove_ExistingInstance_RemovesIt() {
		var mgr = new JobManager();
		var stage = MakeStage("shops");
		var job = MakeJob(stage, new Dictionary<string, string>());
		mgr.Add(job);
		mgr.Remove(job).Should().BeTrue();
		mgr.Exists("shops", new Dictionary<string, string>()).Should().BeFalse();
	}

	[Fact]
	public void InstancesOf_FiltersByStageName() {
		var mgr = new JobManager();
		var stageA = MakeStage("a");
		var stageB = MakeStage("b");
		mgr.Add(MakeJob(stageA, new Dictionary<string, string> { ["k"] = "1" }));
		mgr.Add(MakeJob(stageA, new Dictionary<string, string> { ["k"] = "2" }));
		mgr.Add(MakeJob(stageB, new Dictionary<string, string>()));
		mgr.InstancesOf("a").Should().HaveCount(2);
		mgr.InstancesOf("b").Should().ContainSingle();
		mgr.InstancesOf("nonexistent").Should().BeEmpty();
	}

	[Fact]
	public void ToOverview_ReflectsCurrentState() {
		var mgr = new JobManager();
		var stage = MakeStage("shops");
		var job = MakeJob(stage, new Dictionary<string, string>());
		job.LastSuccess = DateTimeOffset.UtcNow;
		job.ConsecutiveFailures = 2;
		job.LastError = "boom";
		mgr.Add(job);

		var overview = mgr.ToOverview();
		overview.Jobs.Should().ContainSingle();
		var info = overview.Jobs[0];
		info.StageName.Should().Be("shops");
		info.FullyQualifiedName.Should().Be("shops[]");
		info.LastSuccess.Should().NotBeNull();
		info.ConsecutiveFailures.Should().Be(2);
		info.LastError.Should().Be("boom");
	}
}
