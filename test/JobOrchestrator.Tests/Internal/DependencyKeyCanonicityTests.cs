namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// 3.3: encoder canonicity — <see cref="InstanceIdentity.Encode"/> должен давать одинаковую строку
/// для одинаковых key-value-пар независимо от порядка вставки. Это критическое инвариант для
/// hash-equality в <see cref="InstanceManager"/> и <see cref="KeyspaceRegistry"/>:
/// без него один и тот же логический инстанс мог бы получить разные encoded-ключи в разных вызовах
/// и попасть в разные bucket-ы.
/// <para>
/// После Phase C рефакторинга <c>DependencyKey</c>-utility удалён — Encode стал приватной деталью
/// Identity, видимой тестам через <c>InternalsVisibleTo</c>.
/// </para>
/// </summary>
public sealed class DependencyKeyCanonicityTests {
	[Fact]
	public void Encode_DifferentInsertionOrder_ProducesSameEncoding() {
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
		InstanceIdentity.Encode(dict1).Should().Be(InstanceIdentity.Encode(dict2));
		InstanceIdentity.Encode(dict2).Should().Be(InstanceIdentity.Encode(dict3));
	}

	[Fact]
	public void Encode_EscapesSpecialChars() {
		// Без экранирования {a:"1|b=2"} vs {a:"1", b:"2"} дали бы одинаковую encoded-строку.
		var ambiguous = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1|b=2" };
		var twoKeys = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" };
		InstanceIdentity.Encode(ambiguous).Should().NotBe(InstanceIdentity.Encode(twoKeys));
	}

	[Fact]
	public void Encode_EmptyDictionary_ReturnsEmptyString() {
		InstanceIdentity.Encode(new Dictionary<string, string>()).Should().Be(string.Empty);
	}

	[Fact]
	public void InstanceIdentity_DifferentInsertionOrder_AreEqual() {
		var stage = TestStages.Make("x", new() { ServiceType = typeof(object) });
		var keys1 = new Dictionary<string, string>(StringComparer.Ordinal) { ["a"] = "1", ["b"] = "2" };
		var keys2 = new Dictionary<string, string>(StringComparer.Ordinal) { ["b"] = "2", ["a"] = "1" };
		var id1 = new InstanceIdentity(stage, keys1);
		var id2 = new InstanceIdentity(stage, keys2);

		id1.Should().Be(id2, "Identity equality по (StageName, EncodedKey) — порядок не влияет");
		id1.GetHashCode().Should().Be(id2.GetHashCode());
		id1.EncodedKey.Should().Be(id2.EncodedKey);
	}
}
