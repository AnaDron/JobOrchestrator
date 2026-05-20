namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Проверка snapshot-consistency через immutable <see cref="JobMetrics"/>:
/// <see cref="InstanceExecutionStats"/> + <see cref="InstanceSchedule"/> согласованы в одной эпохе.
/// </summary>
public sealed class JobMetricsSnapshotTests {
	private static Instance MakeInstance() {
		var stage = TestStages.Make("x");
		var keys = new Dictionary<string, string>(StringComparer.Ordinal);
		return new Instance { Identity = new InstanceIdentity(stage, keys) };
	}

	[Fact]
	public void Metrics_NewInstance_IsEmpty() {
		var inst = MakeInstance();
		inst.Metrics.Should().Be(JobMetrics.Empty);
		inst.Metrics.Stats.Should().Be(InstanceExecutionStats.Empty);
		inst.Metrics.Schedule.Should().Be(InstanceSchedule.Unscheduled);
		inst.Metrics.Stats.LastSuccess.Should().BeNull();
		inst.Metrics.Stats.LastAttempt.Should().BeNull();
		inst.Metrics.Stats.ConsecutiveFailures.Should().Be(0);
		inst.Metrics.Stats.LastError.Should().BeNull();
		inst.Metrics.Schedule.NextAutoUtc.Should().BeNull();
	}

	[Fact]
	public void SetMetrics_AtomicReplacement_ReadSideSeesAllFields() {
		var inst = MakeInstance();
		var at = DateTimeOffset.UtcNow;
		inst.SetMetrics(new JobMetrics(
			Stats: new InstanceExecutionStats(at, at, 0, null),
			Schedule: new InstanceSchedule(at.AddMinutes(1))));

		var m = inst.Metrics;
		m.Stats.LastSuccess.Should().Be(at);
		m.Stats.LastAttempt.Should().Be(at);
		m.Stats.ConsecutiveFailures.Should().Be(0);
		m.Stats.LastError.Should().BeNull();
		m.Schedule.NextAutoUtc.Should().Be(at.AddMinutes(1));
	}

	[Fact]
	public void SetMetrics_WithExpression_PreservesUntouchedFields() {
		var inst = MakeInstance();
		var t1 = DateTimeOffset.UtcNow.AddMinutes(-10);
		inst.SetMetrics(new JobMetrics(
			Stats: new InstanceExecutionStats(t1, t1, 0, null),
			Schedule: new InstanceSchedule(t1.AddMinutes(1))));

		inst.SetMetrics(inst.Metrics with {
			Stats = inst.Metrics.Stats with { ConsecutiveFailures = 3, LastError = "boom" },
		});

		var m = inst.Metrics;
		m.Stats.LastSuccess.Should().Be(t1, "монотонно: не сброшено");
		m.Stats.LastAttempt.Should().Be(t1);
		m.Stats.ConsecutiveFailures.Should().Be(3);
		m.Stats.LastError.Should().Be("boom");
		m.Schedule.NextAutoUtc.Should().Be(t1.AddMinutes(1));
	}

	[Fact]
	public void SnapshotConsistency_BetweenWritesReaderSeesOnlyConsistentEpochs() {
		var inst = MakeInstance();
		var t1 = DateTimeOffset.UtcNow.AddSeconds(-10);
		var t2 = DateTimeOffset.UtcNow;
		inst.SetMetrics(new JobMetrics(
			Stats: new InstanceExecutionStats(t1, t1, 0, null),
			Schedule: new InstanceSchedule(t1.AddMinutes(1))));
		var snap1 = inst.Metrics;

		inst.SetMetrics(new JobMetrics(
			Stats: new InstanceExecutionStats(t2, t2, 0, null),
			Schedule: new InstanceSchedule(t2.AddMinutes(1))));
		var snap2 = inst.Metrics;

		snap1.Stats.LastSuccess.Should().Be(t1);
		snap1.Schedule.NextAutoUtc.Should().Be(t1.AddMinutes(1));

		snap2.Stats.LastSuccess.Should().Be(t2);
		snap2.Schedule.NextAutoUtc.Should().Be(t2.AddMinutes(1));
	}
}
