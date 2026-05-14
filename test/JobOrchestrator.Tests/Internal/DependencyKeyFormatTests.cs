namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Тесты <c>FullyQualifiedName</c> и canonical <c>EncodedKey</c> через <see cref="InstanceIdentity"/>.
/// Порядок компонентов задаётся <see cref="StageDescriptor.ExpectedKeyNames"/> — pre-sorted один раз
/// в Identity-ctor, и Encode и FormatFqn используют тот же ordered-список.
/// </summary>
public sealed class DependencyKeyFormatTests {
	private static StageDescriptor MakeStage(string name = "x", params string[] expectedKeyNames) =>
		TestStages.Make(name, new() { ExpectedKeyNames = expectedKeyNames });

	[Fact]
	public void Identity_EmptyKeys_FormatsAsBrackets() {
		var id = new InstanceIdentity(MakeStage("shops"));
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
		// FQN-порядок ОПРЕДЕЛЯЕТСЯ stage.ExpectedKeyNames; insertion order словаря НЕ участвует.
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
	public void EncodedKey_EmptyKeys_ReturnsEmpty() {
		new InstanceIdentity(MakeStage("x")).EncodedKey.Should().Be(string.Empty);
	}

	[Fact]
	public void EncodedKey_DifferentValues_DifferentResult() {
		var stage = MakeStage("x", "shops");
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u2" };
		new InstanceIdentity(stage, a).EncodedKey.Should().NotBe(new InstanceIdentity(stage, b).EncodedKey);
	}

	[Fact]
	public void EncodedKey_EscapesBackslash() {
		var stage = MakeStage("x", "a");
		var withBackslash = new Dictionary<string, string> { ["a"] = "value\\with\\backslash" };
		var simple = new Dictionary<string, string> { ["a"] = "value" };
		new InstanceIdentity(stage, withBackslash).EncodedKey
			.Should().NotBe(new InstanceIdentity(stage, simple).EncodedKey);
	}

	[Fact]
	public void EncodedKey_RoundTripStability_SameDictionary_SameEncoded() {
		var stage = MakeStage("x", "shops", "currencies");
		var keys = new Dictionary<string, string> { ["shops"] = "u|1=v", ["currencies"] = "USD" };
		var first = new InstanceIdentity(stage, keys).EncodedKey;
		var second = new InstanceIdentity(stage, keys).EncodedKey;
		first.Should().Be(second);
	}
}
