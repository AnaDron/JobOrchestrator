namespace JobOrchestrator.Tests.Internal;

public sealed class KeyspaceRegistryTests {
	private static readonly StageDescriptor Shops = TestStages.Make("shops");
	private static readonly StageDescriptor Employees = TestStages.Make("employees");

	private static InstanceIdentity Keyless(StageDescriptor stage) =>
		new(stage, new Dictionary<string, string>(StringComparer.Ordinal));

	private static InstanceIdentity Emitter(StageDescriptor stage, string keyName, string keyValue) =>
		new(stage, new Dictionary<string, string>(StringComparer.Ordinal) { [keyName] = keyValue });

	[Fact]
	public void Add_NewKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		reg.Add(shops, "u1").Should().BeTrue();
		reg.Contains(shops, "u1").Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		reg.Add(shops, "u1").Should().BeTrue();
		reg.Add(shops, "u1").Should().BeFalse();
		reg.Contains(shops, "u1").Should().BeTrue();
	}

	[Fact]
	public void Remove_ExistingKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		reg.Add(shops, "u1");
		reg.Remove(shops, "u1").Should().BeTrue();
		reg.Contains(shops, "u1").Should().BeFalse();
	}

	[Fact]
	public void Remove_NonExistentKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		reg.Remove(shops, "u1").Should().BeFalse();
	}

	[Fact]
	public void SnapshotByStage_PopulatedKeylessEmitter_ReturnsAllKeys() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		reg.Add(shops, "u1");
		reg.Add(shops, "u2");
		reg.Add(shops, "u3");
		var buckets = reg.SnapshotByStage("shops").ToList();
		buckets.Should().ContainSingle();
		buckets[0].Emitter.DependencyKeys.Should().BeEmpty();
		buckets[0].Keys.Should().BeEquivalentTo(["u1", "u2", "u3"]);
	}

	[Fact]
	public void Add_DifferentStages_AreIsolated() {
		var reg = new KeyspaceRegistry();
		var shops = Keyless(Shops);
		var employees = Keyless(Employees);
		reg.Add(shops, "u1");
		reg.Add(employees, "u1");
		reg.Contains(shops, "u1").Should().BeTrue();
		reg.Contains(employees, "u1").Should().BeTrue();
		reg.Remove(shops, "u1");
		reg.Contains(employees, "u1").Should().BeTrue();
	}

	[Fact]
	public void MultiInstanceEmitter_IsolatesKeysByEmitterDependencyKeys() {
		// Bug-fix demonstration: shops с DependsOnInstance(regions). Один инстанс shops[regions=EU]
		// эмитит "eu-1", другой shops[regions=US] эмитит "us-1". Ключи живут в РАЗНЫХ bucket-ах.
		var reg = new KeyspaceRegistry();
		var euEmitter = Emitter(Shops, "regions", "EU");
		var usEmitter = Emitter(Shops, "regions", "US");

		reg.Add(euEmitter, "eu-1").Should().BeTrue();
		reg.Add(usEmitter, "us-1").Should().BeTrue();

		reg.Contains(euEmitter, "eu-1").Should().BeTrue();
		reg.Contains(usEmitter, "us-1").Should().BeTrue();
		reg.Contains(euEmitter, "us-1").Should().BeFalse("us-1 не в EU-bucket");
		reg.Contains(usEmitter, "eu-1").Should().BeFalse("eu-1 не в US-bucket");
	}

	[Fact]
	public void RemoveInstance_DropsEntireEmitterBucket_ReturnsOrphanKeys() {
		var reg = new KeyspaceRegistry();
		var emitter = Emitter(Shops, "regions", "EU");
		reg.Add(emitter, "eu-1");
		reg.Add(emitter, "eu-2");
		reg.Add(emitter, "eu-3");

		var orphans = reg.RemoveInstance(emitter);
		orphans.Should().BeEquivalentTo(["eu-1", "eu-2", "eu-3"]);

		reg.Contains(emitter, "eu-1").Should().BeFalse();
		reg.SnapshotByStage("shops").Should().BeEmpty();
	}

	[Fact]
	public void RemoveInstance_PreservesOtherEmittersBuckets() {
		// При удалении одного эмитера multi-instance стадии — bucket другого остаётся нетронутым.
		var reg = new KeyspaceRegistry();
		var eu = Emitter(Shops, "regions", "EU");
		var us = Emitter(Shops, "regions", "US");
		reg.Add(eu, "eu-1");
		reg.Add(us, "us-1");

		reg.RemoveInstance(eu);

		reg.Contains(us, "us-1").Should().BeTrue("US-bucket не затронут");
		reg.Contains(eu, "eu-1").Should().BeFalse();
	}
}
