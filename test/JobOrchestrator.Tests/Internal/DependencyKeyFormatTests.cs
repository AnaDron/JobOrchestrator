namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Тесты canonical encoding (<see cref="DependencyKey.Encode"/>) и формирования <c>FullyQualifiedName</c>
/// через <see cref="InstanceIdentity"/>. FormatFullyQualifiedName-логика — приватная деталь Identity,
/// поэтому проверяется через Identity.FullyQualifiedName.
/// </summary>
public sealed class DependencyKeyFormatTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name = "x", params StageDependency[] deps) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = deps,
	};

	[Fact]
	public void Identity_EmptyKeys_FormatsAsBrackets() {
		var id = new InstanceIdentity(MakeStage("shops"), new Dictionary<string, string>(), []);
		id.FullyQualifiedName.Should().Be("shops[]");
	}

	[Fact]
	public void Identity_SingleKey_NoSpaces() {
		var keys = new Dictionary<string, string> { ["shops"] = "ab12-c34d" };
		var id = new InstanceIdentity(MakeStage("productGroups"), keys, ["shops"]);
		id.FullyQualifiedName.Should().Be("productGroups[shops=ab12-c34d]");
	}

	[Fact]
	public void Identity_MultipleKeys_RespectsOrderedKeyNames() {
		var keys = new Dictionary<string, string> {
			["shops"] = "uuid-1",
			["currencies"] = "USD",
		};
		var id = new InstanceIdentity(MakeStage("prices"), keys, ["shops", "currencies"]);
		id.FullyQualifiedName.Should().Be("prices[shops=uuid-1,currencies=USD]");
	}

	[Fact]
	public void Identity_OrderedKeyNamesDeterminesOrder_NotInsertionOrder() {
		// FQN-порядок ОПРЕДЕЛЯЕТСЯ orderedKeyNames-параметром (transitive ExpectedKeyNames из registry).
		// Insertion order словаря НЕ участвует: Identity конвертирует в ImmutableDictionary, который
		// insertion order не сохраняет.
		var keys = new Dictionary<string, string> {
			["currencies"] = "USD",      // вставлен первым
			["shops"] = "u1",
		};
		var id = new InstanceIdentity(MakeStage("x"), keys, ["shops", "currencies"]);
		id.FullyQualifiedName.Should().Be("x[shops=u1,currencies=USD]");
		id.FullyQualifiedName.Should().NotBe("x[currencies=USD,shops=u1]");
	}

	[Fact]
	public void Identity_MissingFromOrderedList_GoesToSafetyNet() {
		// Edge-case: key, отсутствующий в orderedKeyNames, добавляется в конец.
		var keys = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		var id = new InstanceIdentity(MakeStage("x"), keys, ["a"]);    // b не в ordered
		id.FullyQualifiedName.Should().StartWith("x[a=1");
		id.FullyQualifiedName.Should().Contain("b=2");
	}

	[Fact]
	public void Encode_EmptyKeys_ReturnsEmpty() {
		var enc = DependencyKey.Encode(new Dictionary<string, string>());
		enc.Should().Be(string.Empty);
	}

	[Fact]
	public void Encode_IsCanonical_OrderIndependent() {
		var a = new Dictionary<string, string> { ["shops"] = "u1", ["currencies"] = "USD" };
		var b = new Dictionary<string, string> { ["currencies"] = "USD", ["shops"] = "u1" };
		DependencyKey.Encode(a).Should().Be(DependencyKey.Encode(b));
	}

	[Fact]
	public void Encode_DifferentValues_DifferentResult() {
		var a = new Dictionary<string, string> { ["shops"] = "u1" };
		var b = new Dictionary<string, string> { ["shops"] = "u2" };
		DependencyKey.Encode(a).Should().NotBe(DependencyKey.Encode(b));
	}

	[Fact]
	public void Encode_EscapesSpecialCharacters_NoCollision() {
		var collision1 = new Dictionary<string, string> { ["a"] = "1|b=2" };
		var collision2 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		DependencyKey.Encode(collision1).Should().NotBe(DependencyKey.Encode(collision2));
	}

	[Fact]
	public void Encode_EscapesBackslash() {
		var withBackslash = new Dictionary<string, string> { ["a"] = "value\\with\\backslash" };
		var simple = new Dictionary<string, string> { ["a"] = "value" };
		DependencyKey.Encode(withBackslash).Should().NotBe(DependencyKey.Encode(simple));
	}

	[Fact]
	public void Encode_RoundTripStability_SameDictionary_SameEncoded() {
		var keys = new Dictionary<string, string> { ["shops"] = "u|1=v", ["currencies"] = "USD" };
		var first = DependencyKey.Encode(keys);
		var second = DependencyKey.Encode(keys);
		first.Should().Be(second);
	}
}
