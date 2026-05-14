namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Тесты canonical encoding (<see cref="InstanceIdentity.Encode"/>) и формирования
/// <c>FullyQualifiedName</c> через <see cref="InstanceIdentity"/>. После Phase A/B рефакторинга
/// порядок компонентов FQN задаётся <see cref="StageDescriptor.ExpectedKeyNames"/> — а не отдельным
/// параметром конструктора Identity. Тесты конструируют дескрипторы со «вручную заданным»
/// ExpectedKeyNames, имитируя то, что в production сделал бы <see cref="StageDescriptorGraph.Build"/>.
/// </summary>
public sealed class DependencyKeyFormatTests {
	private static StageDescriptor MakeStage(string name = "x", params string[] expectedKeyNames) =>
		TestStages.Make(name, new() { ExpectedKeyNames = expectedKeyNames });

	[Fact]
	public void Identity_EmptyKeys_FormatsAsBrackets() {
		var id = new InstanceIdentity(MakeStage("shops"), new Dictionary<string, string>());
		id.FullyQualifiedName.Should().Be("shops[]");
	}

	[Fact]
	public void Identity_SingleKey_NoSpaces() {
		var keys = new Dictionary<string, string> { ["shops"] = "ab12-c34d" };
		var id = new InstanceIdentity(MakeStage("productGroups", "shops"), keys);
		id.FullyQualifiedName.Should().Be("productGroups[shops=ab12-c34d]");
	}

	[Fact]
	public void Identity_MultipleKeys_RespectsExpectedKeyNamesOrder() {
		var keys = new Dictionary<string, string> {
			["shops"] = "uuid-1",
			["currencies"] = "USD",
		};
		var id = new InstanceIdentity(MakeStage("prices", "shops", "currencies"), keys);
		id.FullyQualifiedName.Should().Be("prices[shops=uuid-1,currencies=USD]");
	}

	[Fact]
	public void Identity_ExpectedKeyNamesDeterminesOrder_NotInsertionOrder() {
		// FQN-порядок ОПРЕДЕЛЯЕТСЯ stage.ExpectedKeyNames (transitively-computed в StageDescriptorGraph.Build).
		// Insertion order словаря НЕ участвует: Identity конвертирует в ImmutableDictionary, который
		// insertion order не сохраняет.
		var keys = new Dictionary<string, string> {
			["currencies"] = "USD",      // вставлен первым
			["shops"] = "u1",
		};
		var id = new InstanceIdentity(MakeStage("x", "shops", "currencies"), keys);
		id.FullyQualifiedName.Should().Be("x[shops=u1,currencies=USD]");
		id.FullyQualifiedName.Should().NotBe("x[currencies=USD,shops=u1]");
	}

	[Fact]
	public void Identity_MissingFromExpectedKeyNames_GoesToSafetyNet() {
		// Edge-case: key, отсутствующий в ExpectedKeyNames, добавляется в конец.
		var keys = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		var id = new InstanceIdentity(MakeStage("x", "a"), keys);    // b не в expected
		id.FullyQualifiedName.Should().StartWith("x[a=1");
		id.FullyQualifiedName.Should().Contain("b=2");
	}

	[Fact]
	public void Encode_EmptyKeys_ReturnsEmpty() {
		var enc = InstanceIdentity.Encode(new Dictionary<string, string>());
		enc.Should().Be(string.Empty);
	}

	[Fact]
	public void Encode_IsCanonical_OrderIndependent() {
		var a = new Dictionary<string, string> { ["shops"] = "u1", ["currencies"] = "USD" };
		var b = new Dictionary<string, string> { ["currencies"] = "USD", ["shops"] = "u1" };
		InstanceIdentity.Encode(a).Should().Be(InstanceIdentity.Encode(b));
	}

	[Fact]
	public void Encode_DifferentValues_DifferentResult() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u2" };
		InstanceIdentity.Encode(a).Should().NotBe(InstanceIdentity.Encode(b));
	}

	[Fact]
	public void Encode_EscapesSpecialCharacters_NoCollision() {
		var collision1 = new Dictionary<string, string> { ["a"] = "1|b=2" };
		var collision2 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		InstanceIdentity.Encode(collision1).Should().NotBe(InstanceIdentity.Encode(collision2));
	}

	[Fact]
	public void Encode_EscapesBackslash() {
		var withBackslash = new Dictionary<string, string> { ["a"] = "value\\with\\backslash" };
		var simple = new Dictionary<string, string> { ["a"] = "value" };
		InstanceIdentity.Encode(withBackslash).Should().NotBe(InstanceIdentity.Encode(simple));
	}

	[Fact]
	public void Encode_RoundTripStability_SameDictionary_SameEncoded() {
		var keys = new Dictionary<string, string> { ["shops"] = "u|1=v", ["currencies"] = "USD" };
		var first = InstanceIdentity.Encode(keys);
		var second = InstanceIdentity.Encode(keys);
		first.Should().Be(second);
	}
}
