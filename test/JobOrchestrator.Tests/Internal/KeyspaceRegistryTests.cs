namespace JobOrchestrator.Tests.Internal;

public sealed class KeyspaceRegistryTests {
	private static readonly IReadOnlyDictionary<string, string> Empty =
		new Dictionary<string, string>(StringComparer.Ordinal);

	private static IReadOnlyDictionary<string, string> Emitter(string name, string value) =>
		new Dictionary<string, string>(StringComparer.Ordinal) { [name] = value };

	[Fact]
	public void Add_NewKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", Empty, "u1").Should().BeTrue();
		reg.Contains("shops", Empty, "u1").Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", Empty, "u1").Should().BeTrue();
		reg.Add("shops", Empty, "u1").Should().BeFalse();
		reg.Contains("shops", Empty, "u1").Should().BeTrue();
	}

	[Fact]
	public void Remove_ExistingKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", Empty, "u1");
		reg.Remove("shops", Empty, "u1").Should().BeTrue();
		reg.Contains("shops", Empty, "u1").Should().BeFalse();
	}

	[Fact]
	public void Remove_NonExistentKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		reg.Remove("shops", Empty, "u1").Should().BeFalse();
		reg.Remove("nonexistent-stage", Empty, "k").Should().BeFalse();
	}

	[Fact]
	public void SnapshotByStage_PopulatedKeylessEmitter_ReturnsAllKeys() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", Empty, "u1");
		reg.Add("shops", Empty, "u2");
		reg.Add("shops", Empty, "u3");
		var buckets = reg.SnapshotByStage("shops").ToList();
		buckets.Should().ContainSingle();
		buckets[0].EmitterKeys.Should().BeEmpty();
		buckets[0].Keys.Should().BeEquivalentTo(["u1", "u2", "u3"]);
	}

	[Fact]
	public void Add_DifferentStages_AreIsolated() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", Empty, "u1");
		reg.Add("employees", Empty, "u1");
		reg.Contains("shops", Empty, "u1").Should().BeTrue();
		reg.Contains("employees", Empty, "u1").Should().BeTrue();
		reg.Remove("shops", Empty, "u1");
		reg.Contains("employees", Empty, "u1").Should().BeTrue();
	}

	[Fact]
	public void MultiInstanceEmitter_IsolatesKeysByEmitterDependencyKeys() {
		// Bug-fix demonstration: shops с DependsOnInstance(regions). Один инстанс shops[regions=EU]
		// эмитит "eu-1", другой shops[regions=US] эмитит "us-1". Ключи живут в РАЗНЫХ bucket-ах.
		var reg = new KeyspaceRegistry();
		var euEmitter = Emitter("regions", "EU");
		var usEmitter = Emitter("regions", "US");

		reg.Add("shops", euEmitter, "eu-1").Should().BeTrue();
		reg.Add("shops", usEmitter, "us-1").Should().BeTrue();

		reg.Contains("shops", euEmitter, "eu-1").Should().BeTrue();
		reg.Contains("shops", usEmitter, "us-1").Should().BeTrue();
		reg.Contains("shops", euEmitter, "us-1").Should().BeFalse("us-1 не в EU-bucket");
		reg.Contains("shops", usEmitter, "eu-1").Should().BeFalse("eu-1 не в US-bucket");
	}

	[Fact]
	public void RemoveInstance_DropsEntireEmitterBucket_ReturnsOrphanKeys() {
		var reg = new KeyspaceRegistry();
		var emitter = Emitter("regions", "EU");
		reg.Add("shops", emitter, "eu-1");
		reg.Add("shops", emitter, "eu-2");
		reg.Add("shops", emitter, "eu-3");

		var orphans = reg.RemoveInstance("shops", emitter);
		orphans.Should().BeEquivalentTo(["eu-1", "eu-2", "eu-3"]);

		reg.Contains("shops", emitter, "eu-1").Should().BeFalse();
		reg.SnapshotByStage("shops").Should().BeEmpty();
	}

	[Fact]
	public void RemoveInstance_PreservesOtherEmittersBuckets() {
		// При удалении одного эмитера multi-instance стадии — bucket другого остаётся нетронутым.
		var reg = new KeyspaceRegistry();
		var eu = Emitter("regions", "EU");
		var us = Emitter("regions", "US");
		reg.Add("shops", eu, "eu-1");
		reg.Add("shops", us, "us-1");

		reg.RemoveInstance("shops", eu);

		reg.Contains("shops", us, "us-1").Should().BeTrue("US-bucket не затронут");
		reg.Contains("shops", eu, "eu-1").Should().BeFalse();
	}
}
