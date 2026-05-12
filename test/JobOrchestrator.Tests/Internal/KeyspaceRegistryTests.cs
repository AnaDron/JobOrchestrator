namespace JobOrchestrator.Tests.Internal;

public sealed class KeyspaceRegistryTests {
	[Fact]
	public void Add_NewKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", "u1").Should().BeTrue();
		reg.Contains("shops", "u1").Should().BeTrue();
	}

	[Fact]
	public void Add_DuplicateKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", "u1").Should().BeTrue();
		reg.Add("shops", "u1").Should().BeFalse();
		reg.Contains("shops", "u1").Should().BeTrue();
	}

	[Fact]
	public void Remove_ExistingKey_ReturnsTrue() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", "u1");
		reg.Remove("shops", "u1").Should().BeTrue();
		reg.Contains("shops", "u1").Should().BeFalse();
	}

	[Fact]
	public void Remove_NonExistentKey_ReturnsFalseIdempotent() {
		var reg = new KeyspaceRegistry();
		reg.Remove("shops", "u1").Should().BeFalse();
		reg.Remove("nonexistent-stage", "k").Should().BeFalse();
	}

	[Fact]
	public void Get_UnknownStage_ReturnsEmpty() {
		var reg = new KeyspaceRegistry();
		reg.Get("shops").Should().BeEmpty();
	}

	[Fact]
	public void Get_PopulatedStage_ReturnsAllKeys() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", "u1");
		reg.Add("shops", "u2");
		reg.Add("shops", "u3");
		reg.Get("shops").Should().BeEquivalentTo("u1", "u2", "u3");
	}

	[Fact]
	public void Add_DifferentStages_AreIsolated() {
		var reg = new KeyspaceRegistry();
		reg.Add("shops", "u1");
		reg.Add("employees", "u1");
		reg.Contains("shops", "u1").Should().BeTrue();
		reg.Contains("employees", "u1").Should().BeTrue();
		reg.Remove("shops", "u1");
		reg.Contains("employees", "u1").Should().BeTrue();
	}
}
