namespace JobOrchestrator.Tests.Internal;

public sealed class DependencyKeyFormatTests {
	[Fact]
	public void FormatFullyQualifiedName_EmptyKeys_FormatsAsBrackets() {
		var fqn = DependencyKey.FormatFullyQualifiedName("shops", new Dictionary<string, string>());
		fqn.Should().Be("shops[]");
	}

	[Fact]
	public void FormatFullyQualifiedName_SingleKey_NoSpaces() {
		var keys = new Dictionary<string, string> { ["shops"] = "ab12-c34d" };
		var fqn = DependencyKey.FormatFullyQualifiedName("productGroups", keys);
		fqn.Should().Be("productGroups[shops=ab12-c34d]");
	}

	[Fact]
	public void FormatFullyQualifiedName_MultipleKeys_NoSpacesAfterCommas() {
		var keys = new Dictionary<string, string> {
			["shops"] = "uuid-1",
			["currencies"] = "USD",
		};
		var fqn = DependencyKey.FormatFullyQualifiedName("prices", keys);
		fqn.Should().Be("prices[shops=uuid-1,currencies=USD]");
	}

	[Fact]
	public void FormatFullyQualifiedName_RespectsInsertionOrder_NotAlphabeticalOrder() {
		// SDK при создании инстанса вставляет компоненты в порядке объявления зависимостей в Fluent API.
		// Этот тест — анти-регрессия: если кто-то добавит .OrderBy(k => k.Key) в форматтер, тест упадёт,
		// потому что declaration order ≠ alphabetical order для этих имён.
		var declarationOrderKeys = new Dictionary<string, string> {
			["shops"] = "u1",
			["currencies"] = "USD",  // алфавитно был бы первым
		};
		var fqn = DependencyKey.FormatFullyQualifiedName("x", declarationOrderKeys);
		fqn.Should().Be("x[shops=u1,currencies=USD]");
		fqn.Should().NotBe("x[currencies=USD,shops=u1]");
	}

	[Fact]
	public void Encode_EmptyKeys_ReturnsEmpty() {
		var enc = DependencyKey.Encode(new Dictionary<string, string>());
		enc.Should().Be(string.Empty);
	}

	[Fact]
	public void Encode_IsCanonical_OrderIndependent() {
		// Encode — каноничен (sort by name), используется для словарного ключа JobManager-а:
		// два словаря с одинаковым содержимым, но разным порядком вставки, должны дать одну строку.
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
		// Без escaping {a: "1|b=2"} и {a: "1", b: "2"} дали бы одинаковую encoded-строку.
		// Escaping (|, =, \ через \) гарантирует уникальность.
		var collision1 = new Dictionary<string, string> { ["a"] = "1|b=2" };
		var collision2 = new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" };
		DependencyKey.Encode(collision1).Should().NotBe(DependencyKey.Encode(collision2));
	}

	[Fact]
	public void Encode_EscapesBackslash() {
		// Сам символ \ тоже должен экранироваться, иначе {a: "\|b"} коллидировал бы с {a: "", b: ""}-вариантом.
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
