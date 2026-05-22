namespace JobOrchestrator.Tests.Abstractions;

/// <summary>
/// Покрытие <see cref="InstanceKeys"/> — equality по encoded-string, implicit-конверсии, boxing-сценарий.
/// Equality assert'ы делаются через <see cref="IEquatable{T}.Equals"/>, потому что FluentAssertions
/// для struct, реализующего <see cref="IReadOnlyDictionary{TKey, TValue}"/>, по умолчанию выбирает
/// dictionary-assertions, у которых нет <c>Be(...)</c>.
/// </summary>
public sealed class InstanceKeysTests {
	[Fact]
	public void Empty_DefaultStruct_EqualToEmpty() {
		InstanceKeys d = default;
		d.Equals(InstanceKeys.Empty).Should().BeTrue();
		d.GetHashCode().Should().Be(InstanceKeys.Empty.GetHashCode());
		d.Count.Should().Be(0);
	}

	[Fact]
	public void Equality_SameEntries_DifferentInsertOrder_AreEqual() {
		var a = new InstanceKeys(("a", "1"), ("b", "2"));
		var b = new InstanceKeys(("b", "2"), ("a", "1"));
		a.Equals(b).Should().BeTrue(because: "ordinal-sort по имени даёт идентичный encoded-string");
		a.GetHashCode().Should().Be(b.GetHashCode());
		(a == b).Should().BeTrue();
	}

	[Fact]
	public void Equality_DifferentValues_NotEqual() {
		var a = new InstanceKeys(("k", "1"));
		var b = new InstanceKeys(("k", "2"));
		(a == b).Should().BeFalse();
		(a != b).Should().BeTrue();
	}

	[Fact]
	public void Encoding_SpecialCharacters_NoCollision() {
		// Ключи с '|', '=', '\\' экранируются — collision'ы вида {a:"1|b=2"} vs {a:"1",b:"2"} невозможны.
		var collisionAttempt = new InstanceKeys(("a", "1|b=2"));
		var actualTwoKeys = new InstanceKeys(("a", "1"), ("b", "2"));
		collisionAttempt.Equals(actualTwoKeys).Should().BeFalse();
		collisionAttempt.GetHashCode().Should().NotBe(actualTwoKeys.GetHashCode());
	}

	[Fact]
	public void ImplicitConversion_FromTuple_Works() {
		InstanceKeys keys = ("shops", "u1");
		keys.Count.Should().Be(1);
		keys["shops"].Should().Be("u1");
	}

	[Fact]
	public void ImplicitConversion_FromSpan_Works() {
		ReadOnlySpan<(string, string)> span = [("a", "1"), ("b", "2")];
		InstanceKeys keys = span;
		keys.Count.Should().Be(2);
		keys["a"].Should().Be("1");
		keys["b"].Should().Be("2");
	}

	[Fact]
	public void DuplicateName_ThrowsArgumentException() {
		Action act = () => _ = new InstanceKeys(("k", "v1"), ("k", "v2"));
		act.Should().Throw<ArgumentException>().WithMessage("*'k'*");
	}

	[Fact]
	public void EmptyName_ThrowsArgumentException() {
		Action act = () => _ = new InstanceKeys(("", "v"));
		act.Should().Throw<ArgumentException>();
	}

	[Fact]
	public void NullValue_ThrowsArgumentNullException() {
		Action act = () => _ = new InstanceKeys(("k", null!));
		act.Should().Throw<ArgumentNullException>();
	}

	[Fact]
	public void Boxing_ToIReadOnlyDictionary_PreservesContent() {
		// Cast struct → interface boxes в heap; содержимое (entries) остаётся читаемым через интерфейс.
		var keys = new InstanceKeys(("a", "1"), ("b", "2"));
		IReadOnlyDictionary<string, string> boxed = keys;
		boxed.Count.Should().Be(2);
		boxed["a"].Should().Be("1");
		boxed.ContainsKey("b").Should().BeTrue();
		var pairs = boxed.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToArray();
		pairs[0].Should().Be(new KeyValuePair<string, string>("a", "1"));
		pairs[1].Should().Be(new KeyValuePair<string, string>("b", "2"));
	}

	[Fact]
	public void HashSet_GroupsByEquivalentKeys() {
		var set = new HashSet<InstanceKeys> {
			new(("a", "1")),
			new(("a", "1")),                          // дубликат
			new(("a", "1"), ("b", "2")),
			new(("b", "2"), ("a", "1")),              // эквивалент предыдущему
		};
		set.Should().HaveCount(2, because: "value-equality сворачивает эквивалентные ключи");
	}

	[Fact]
	public void TryGetValue_MissingKey_ReturnsFalse() {
		var keys = new InstanceKeys(("k", "v"));
		keys.TryGetValue("k", out var v).Should().BeTrue();
		v.Should().Be("v");
		keys.TryGetValue("other", out _).Should().BeFalse();
	}
}
