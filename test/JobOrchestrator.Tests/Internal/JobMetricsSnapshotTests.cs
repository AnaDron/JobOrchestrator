namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Проверка snapshot-consistency через immutable <see cref="JobMetrics"/>: read-side получает
/// согласованную пятёрку полей из одной «эпохи» writer'а, не торн-сборку.
/// </summary>
public sealed class JobMetricsSnapshotTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageInstance MakeInstance() {
		var stage = new StageDescriptor {
			Name = "x",
			ServiceType = typeof(FakeService),
			Interval = TimeSpan.FromMinutes(1),
			RetryPolicy = RetryPolicy.NoRetry,
			Debounce = TimeSpan.Zero,
			Dependencies = [],
		};
		var keys = new Dictionary<string, string>(StringComparer.Ordinal);
		return new StageInstance { Identity = new InstanceIdentity(stage, keys) };
	}

	[Fact]
	public void Metrics_NewInstance_IsEmpty() {
		var inst = MakeInstance();
		inst.Metrics.Should().Be(JobMetrics.Empty);
		inst.Metrics.LastSuccess.Should().BeNull();
		inst.Metrics.LastAttempt.Should().BeNull();
		inst.Metrics.ConsecutiveFailures.Should().Be(0);
		inst.Metrics.LastError.Should().BeNull();
		inst.Metrics.NextAutoUtc.Should().BeNull();
	}

	[Fact]
	public void SetMetrics_AtomicReplacement_ReadSideSeesAllFields() {
		var inst = MakeInstance();
		var at = DateTimeOffset.UtcNow;
		// Один SetMetrics — все 5 полей становятся видимыми одновременно для read-side.
		inst.SetMetrics(new JobMetrics(
			LastSuccess: at,
			LastAttempt: at,
			ConsecutiveFailures: 0,
			LastError: null,
			NextAutoUtc: at.AddMinutes(1)));

		// Read через Metrics — атомарный snapshot.
		var m = inst.Metrics;
		m.LastSuccess.Should().Be(at);
		m.LastAttempt.Should().Be(at);
		m.ConsecutiveFailures.Should().Be(0);
		m.LastError.Should().BeNull();
		m.NextAutoUtc.Should().Be(at.AddMinutes(1));
	}

	[Fact]
	public void SetMetrics_WithExpression_PreservesUntouchedFields() {
		// Типовой паттерн writer'а: `inst.SetMetrics(inst.Metrics with { Field = newValue })`.
		// Остальные поля должны остаться неизменными.
		var inst = MakeInstance();
		var t1 = DateTimeOffset.UtcNow.AddMinutes(-10);
		inst.SetMetrics(new JobMetrics(LastSuccess: t1, LastAttempt: t1, ConsecutiveFailures: 0, LastError: null, NextAutoUtc: t1.AddMinutes(1)));

		// Обновляем только ConsecutiveFailures и LastError, остальные — наследуются.
		inst.SetMetrics(inst.Metrics with { ConsecutiveFailures = 3, LastError = "boom" });

		var m = inst.Metrics;
		m.LastSuccess.Should().Be(t1, "монотонно: не сброшено");
		m.LastAttempt.Should().Be(t1);
		m.ConsecutiveFailures.Should().Be(3);
		m.LastError.Should().Be("boom");
		m.NextAutoUtc.Should().Be(t1.AddMinutes(1));
	}

	[Fact]
	public void SnapshotConsistency_BetweenWritesReaderSeesOnlyConsistentEpochs() {
		// Между двумя SetMetrics-вызовами writer'а — reader-snapshot всегда содержит ВСЕ поля
		// одной эпохи (т.е. либо полное состояние ДО, либо полное состояние ПОСЛЕ — никогда mix).
		var inst = MakeInstance();
		var t1 = DateTimeOffset.UtcNow.AddSeconds(-10);
		var t2 = DateTimeOffset.UtcNow;
		inst.SetMetrics(new JobMetrics(t1, t1, 0, null, t1.AddMinutes(1)));
		// Snapshot1 — все поля из эпохи t1.
		var snap1 = inst.Metrics;

		inst.SetMetrics(new JobMetrics(t2, t2, 0, null, t2.AddMinutes(1)));
		// Snapshot2 — все поля из эпохи t2.
		var snap2 = inst.Metrics;

		// snap1 не «сдвинулся» при последующем write — это immutable record, безопасно держать ссылку.
		snap1.LastSuccess.Should().Be(t1);
		snap1.NextAutoUtc.Should().Be(t1.AddMinutes(1));

		snap2.LastSuccess.Should().Be(t2);
		snap2.NextAutoUtc.Should().Be(t2.AddMinutes(1));
	}
}
