namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Тесты per-emitter keyspace, который раньше жил в <c>KeyspaceRegistry</c>, а теперь — поле
/// <see cref="Instance.EmittedKeys"/>. Покрытие сохранено 1-в-1 для регрессий cascade/multi-instance.
/// </summary>
public sealed class InstanceEmittedKeysTests {
	private static readonly StageDescriptor Shops = TestStages.Make("shops");
	private static readonly StageDescriptor Employees = TestStages.Make("employees");

	private static Instance MakeInstance(StageDescriptor stage, Dictionary<string, string>? keys = null) =>
		new() { Identity = new InstanceIdentity(stage, keys ?? new Dictionary<string, string>(StringComparer.Ordinal)) };

	[Fact]
	public void Add_NewKey_ReturnsTrue() {
		var shops = MakeInstance(Shops);
		shops.AddEmittedKey("u1").Should().BeTrue();
		shops.ContainsEmittedKey("u1").Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateKey_ReturnsFalseIdempotent() {
		var shops = MakeInstance(Shops);
		shops.AddEmittedKey("u1").Should().BeTrue();
		shops.AddEmittedKey("u1").Should().BeFalse();
		shops.ContainsEmittedKey("u1").Should().BeTrue();
	}

	[Fact]
	public void Remove_ExistingKey_ReturnsTrue() {
		var shops = MakeInstance(Shops);
		shops.AddEmittedKey("u1");
		shops.RemoveEmittedKey("u1").Should().BeTrue();
		shops.ContainsEmittedKey("u1").Should().BeFalse();
	}

	[Fact]
	public void Remove_NonExistentKey_ReturnsFalseIdempotent() {
		var shops = MakeInstance(Shops);
		shops.RemoveEmittedKey("u1").Should().BeFalse();
	}

	[Fact]
	public void EmittedKeys_PopulatedKeylessEmitter_ReturnsAllKeys() {
		var shops = MakeInstance(Shops);
		shops.AddEmittedKey("u1");
		shops.AddEmittedKey("u2");
		shops.AddEmittedKey("u3");
		shops.EmittedKeys.Should().BeEquivalentTo(["u1", "u2", "u3"]);
	}

	[Fact]
	public void Add_DifferentStages_AreIsolated() {
		// Раньше изоляцию обеспечивал per-stage index в KeyspaceRegistry; теперь — отдельный HashSet
		// на каждом Instance. Разные инстансы по определению не делят keyspace-bucket.
		var shops = MakeInstance(Shops);
		var employees = MakeInstance(Employees);
		shops.AddEmittedKey("u1");
		employees.AddEmittedKey("u1");
		shops.ContainsEmittedKey("u1").Should().BeTrue();
		employees.ContainsEmittedKey("u1").Should().BeTrue();
		shops.RemoveEmittedKey("u1");
		employees.ContainsEmittedKey("u1").Should().BeTrue();
	}

	[Fact]
	public void MultiInstanceEmitter_IsolatesKeysByEmitterDependencyKeys() {
		// Bug-fix demonstration: shops с DependsOnInstance(regions). Один инстанс shops[regions=EU]
		// эмитит "eu-1", другой shops[regions=US] эмитит "us-1". Ключи живут на РАЗНЫХ Instance-объектах.
		var euEmitter = MakeInstance(Shops, new Dictionary<string, string> { ["regions"] = "EU" });
		var usEmitter = MakeInstance(Shops, new Dictionary<string, string> { ["regions"] = "US" });

		euEmitter.AddEmittedKey("eu-1").Should().BeTrue();
		usEmitter.AddEmittedKey("us-1").Should().BeTrue();

		euEmitter.ContainsEmittedKey("eu-1").Should().BeTrue();
		usEmitter.ContainsEmittedKey("us-1").Should().BeTrue();
		euEmitter.ContainsEmittedKey("us-1").Should().BeFalse("us-1 не в EU-bucket");
		usEmitter.ContainsEmittedKey("eu-1").Should().BeFalse("eu-1 не в US-bucket");
	}

	[Fact]
	public void TakeEmittedKeys_DrainsBucket_ReturnsOrphanKeys() {
		var emitter = MakeInstance(Shops, new Dictionary<string, string> { ["regions"] = "EU" });
		emitter.AddEmittedKey("eu-1");
		emitter.AddEmittedKey("eu-2");
		emitter.AddEmittedKey("eu-3");

		var orphans = emitter.TakeEmittedKeys();
		orphans.Should().BeEquivalentTo(["eu-1", "eu-2", "eu-3"]);

		emitter.ContainsEmittedKey("eu-1").Should().BeFalse();
		emitter.EmittedKeys.Should().BeEmpty();
	}

	[Fact]
	public void TakeEmittedKeys_DoesNotAffectOtherEmitters() {
		// При cascade-removal одного эмитера multi-instance стадии — keyspace другого остаётся нетронутым.
		var eu = MakeInstance(Shops, new Dictionary<string, string> { ["regions"] = "EU" });
		var us = MakeInstance(Shops, new Dictionary<string, string> { ["regions"] = "US" });
		eu.AddEmittedKey("eu-1");
		us.AddEmittedKey("us-1");

		eu.TakeEmittedKeys();

		us.ContainsEmittedKey("us-1").Should().BeTrue("US-bucket не затронут");
		eu.ContainsEmittedKey("eu-1").Should().BeFalse();
	}
}
