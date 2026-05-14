namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Encoder canonicity через <see cref="InstanceIdentity"/>: для одинаковых key-value-пар в пределах
/// одной стадии (= одинаковый <see cref="StageDescriptor.ExpectedKeyNames"/>) <c>EncodedKey</c>
/// одинаков независимо от insertion-order словаря. Это критический инвариант для hash-equality в
/// <see cref="InstanceManager"/>, <see cref="KeyspaceRegistry"/> и waiters.
/// </summary>
public sealed class DependencyKeyCanonicityTests {
	private static StageDescriptor StageWithKeys(params string[] expectedKeyNames) =>
		TestStages.Make("x", new() { ServiceType = typeof(object), ExpectedKeyNames = expectedKeyNames });

	[Fact]
	public void EncodedKey_DifferentInsertionOrder_ProducesSameEncoding() {
		var stage = StageWithKeys("shops", "currency", "region");
		var dict1 = new Dictionary<string, string>(StringComparer.Ordinal) {
			["shops"] = "u1",
			["currency"] = "USD",
			["region"] = "EU",
		};
		var dict2 = new Dictionary<string, string>(StringComparer.Ordinal) {
			["region"] = "EU",
			["currency"] = "USD",
			["shops"] = "u1",
		};
		var dict3 = new Dictionary<string, string>(StringComparer.Ordinal) {
			["currency"] = "USD",
			["shops"] = "u1",
			["region"] = "EU",
		};

		var enc1 = new InstanceIdentity(stage, dict1).EncodedKey;
		var enc2 = new InstanceIdentity(stage, dict2).EncodedKey;
		var enc3 = new InstanceIdentity(stage, dict3).EncodedKey;
		enc1.Should().Be(enc2);
		enc2.Should().Be(enc3);
	}

	[Fact]
	public void EncodedKey_EscapesSpecialChars_NoCollision() {
		// Без экранирования {a:"1|b=2"} vs {a:"1", b:"2"} дали бы одинаковую encoded-строку.
		var stageA = StageWithKeys("a");
		var stageAB = StageWithKeys("a", "b");

		var ambiguous = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1|b=2" };
		var twoKeys = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" };

		var encAmbiguous = new InstanceIdentity(stageA, ambiguous).EncodedKey;
		var encTwoKeys = new InstanceIdentity(stageAB, twoKeys).EncodedKey;
		encAmbiguous.Should().NotBe(encTwoKeys);
	}

	[Fact]
	public void EncodedKey_EmptyDictionary_ReturnsEmptyString() {
		var stage = TestStages.Make("x", new() { ServiceType = typeof(object) });
		new InstanceIdentity(stage).EncodedKey.Should().Be(string.Empty);
	}

	[Fact]
	public void InstanceIdentity_DifferentInsertionOrder_AreEqual() {
		var stage = StageWithKeys("a", "b");
		var keys1 = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" };
		var keys2 = new Dictionary<string, string>(StringComparer.Ordinal) { ["b"] = "2", ["a"] = "1" };
		var id1 = new InstanceIdentity(stage, keys1);
		var id2 = new InstanceIdentity(stage, keys2);

		id1.Should().Be(id2, "Identity equality по (StageName, EncodedKey) — порядок не влияет");
		id1.GetHashCode().Should().Be(id2.GetHashCode());
		id1.EncodedKey.Should().Be(id2.EncodedKey);
	}
}
