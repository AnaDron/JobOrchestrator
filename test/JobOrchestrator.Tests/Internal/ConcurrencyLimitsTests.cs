namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Per-stage семафоры: TryAcquire возвращает true, пока не выбран лимит, дальше false; Release возвращает токен.
/// </summary>
public sealed class ConcurrencyLimitsTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name, int? concurrencyLimit = null) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = [],
		ConcurrencyLimit = concurrencyLimit,
	};

	[Fact]
	public void StageWithoutLimit_TryAcquireAlwaysTrue() {
		var registry = new StageRegistry([MakeStage("a")]);
		var limits = new ConcurrencyLimits(registry);

		// Сколько бы ни вызывали — всегда true (нет семафора).
		for (int i = 0; i < 10; i++) {
			limits.TryAcquire("a").Should().BeTrue();
		}
	}

	[Fact]
	public void StageWithLimit_AcquireUpToLimit_ThenFalse() {
		var registry = new StageRegistry([MakeStage("a", concurrencyLimit: 3)]);
		var limits = new ConcurrencyLimits(registry);

		limits.TryAcquire("a").Should().BeTrue();    // 1/3
		limits.TryAcquire("a").Should().BeTrue();    // 2/3
		limits.TryAcquire("a").Should().BeTrue();    // 3/3
		limits.TryAcquire("a").Should().BeFalse("лимит выбран");
		limits.TryAcquire("a").Should().BeFalse();
	}

	[Fact]
	public void StageWithLimit_ReleaseRestoresToken() {
		var registry = new StageRegistry([MakeStage("a", concurrencyLimit: 2)]);
		var limits = new ConcurrencyLimits(registry);

		limits.TryAcquire("a").Should().BeTrue();
		limits.TryAcquire("a").Should().BeTrue();
		limits.TryAcquire("a").Should().BeFalse();

		limits.Release("a");
		limits.TryAcquire("a").Should().BeTrue("после release токен снова доступен");
		limits.TryAcquire("a").Should().BeFalse();
	}

	[Fact]
	public void Release_StageWithoutLimit_IsNoOp() {
		var registry = new StageRegistry([MakeStage("a")]);
		var limits = new ConcurrencyLimits(registry);

		// Не должно throws.
		Action act = () => limits.Release("a");
		act.Should().NotThrow();
	}

	[Fact]
	public void Limits_DifferentStages_AreIsolated() {
		var registry = new StageRegistry([
			MakeStage("a", concurrencyLimit: 1),
			MakeStage("b", concurrencyLimit: 1),
		]);
		var limits = new ConcurrencyLimits(registry);

		limits.TryAcquire("a").Should().BeTrue();
		limits.TryAcquire("a").Should().BeFalse();
		// b — отдельный семафор.
		limits.TryAcquire("b").Should().BeTrue();
		limits.TryAcquire("b").Should().BeFalse();
	}
}
