namespace JobOrchestrator.Tests.Configuration;

public sealed class RetryPolicyTests {
	[Fact]
	public void NoRetry_AlwaysReturnsZero() {
		var policy = RetryPolicy.NoRetry;
		policy.ComputeDelay(0).Should().Be(TimeSpan.Zero);
		policy.ComputeDelay(1).Should().Be(TimeSpan.Zero);
		policy.ComputeDelay(10).Should().Be(TimeSpan.Zero);
	}

	[Fact]
	public void FixedDelay_AppliesAfterFailureOnly() {
		var policy = RetryPolicy.FixedDelay(TimeSpan.FromSeconds(5));
		policy.ComputeDelay(0).Should().Be(TimeSpan.Zero);
		policy.ComputeDelay(1).Should().Be(TimeSpan.FromSeconds(5));
		policy.ComputeDelay(3).Should().Be(TimeSpan.FromSeconds(5));
	}

	[Fact]
	public void FixedDelay_NegativeDelay_ThrowsOnFactory() {
		Action act = () => RetryPolicy.FixedDelay(TimeSpan.FromSeconds(-1));
		act.Should().Throw<ArgumentOutOfRangeException>();
	}

	[Fact]
	public void ExponentialBackoff_DoublesUntilCap() {
		var policy = RetryPolicy.ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));
		policy.ComputeDelay(0).Should().Be(TimeSpan.Zero);
		policy.ComputeDelay(1).Should().Be(TimeSpan.FromSeconds(1));
		policy.ComputeDelay(2).Should().Be(TimeSpan.FromSeconds(2));
		policy.ComputeDelay(3).Should().Be(TimeSpan.FromSeconds(4));
		policy.ComputeDelay(4).Should().Be(TimeSpan.FromSeconds(8));
		// 1s * 2^9 = 512s, но cap = 60s → возвращает 60s.
		policy.ComputeDelay(10).Should().Be(TimeSpan.FromMinutes(1));
		policy.ComputeDelay(100).Should().Be(TimeSpan.FromMinutes(1));
	}

	[Fact]
	public void ExponentialBackoff_InvalidArgs_Throws() {
		Action zero = () => RetryPolicy.ExponentialBackoff(TimeSpan.Zero, TimeSpan.FromMinutes(1));
		zero.Should().Throw<ArgumentOutOfRangeException>();

		Action maxLessThanInitial = () => RetryPolicy.ExponentialBackoff(TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(1));
		maxLessThanInitial.Should().Throw<ArgumentOutOfRangeException>();
	}
}
