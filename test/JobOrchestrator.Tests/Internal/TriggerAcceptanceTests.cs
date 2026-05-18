namespace JobOrchestrator.Tests.Internal;

public sealed class TriggerAcceptanceTests {
	private static StageDescriptor MakeStage(TimeSpan debounce, RetryPolicy retry) =>
		TestStages.Make("x", new() { Debounce = debounce, RetryPolicy = retry });

	private static Instance MakeInstance(StageDescriptor stage) =>
		new() { Identity = new InstanceIdentity(stage) };

	private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

	[Fact]
	public void Running_AlwaysRejectedWithAlreadyRunning() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		inst.TryBeginRunning();
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Rejection.Should().Be(TriggerResult.AlreadyRunning);
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Rejection.Should().Be(TriggerResult.AlreadyRunning);
	}

	[Fact]
	public void Idle_NoHistory_AutoAndManualAccepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).IsAccepted.Should().BeTrue();
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).IsAccepted.Should().BeTrue();
	}

	[Fact]
	public void Terminating_RejectedWithTerminating() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		inst.MarkTerminating();
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Rejection.Should().Be(TriggerResult.Terminating);
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Rejection.Should().Be(TriggerResult.Terminating);
	}

	[Fact]
	public void Auto_InRetryDelay_ReturnsWaitingRetry() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.SetMetrics(inst.Metrics with { ConsecutiveFailures = 1 });
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddMinutes(-2) });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Rejection.Should().Be(TriggerResult.WaitingRetry);
	}

	[Fact]
	public void Auto_RetryDelayElapsed_Accepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.SetMetrics(inst.Metrics with { ConsecutiveFailures = 1 });
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddMinutes(-10) });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).IsAccepted.Should().BeTrue();
	}

	[Fact]
	public void Manual_IgnoresRetryDelay() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.SetMetrics(inst.Metrics with { ConsecutiveFailures = 5 });
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddSeconds(-1) });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).IsAccepted.Should().BeTrue();
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterSuccess_Rejected() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddSeconds(-5) });
		inst.SetMetrics(inst.Metrics with { LastSuccess = inst.Metrics.LastAttempt });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Rejection.Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterFailure_AlsoRejected() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddSeconds(-5) });
		inst.SetMetrics(inst.Metrics with { ConsecutiveFailures = 1 });
		inst.SetMetrics(inst.Metrics with { LastError = "boom" });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Rejection.Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_DebounceElapsed_Accepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddSeconds(-30) });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).IsAccepted.Should().BeTrue();
	}

	[Fact]
	public void Auto_DebounceNotChecked() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromMinutes(1), RetryPolicy.NoRetry));
		inst.SetMetrics(inst.Metrics with { LastAttempt = Now.AddSeconds(-1) });
		inst.SetMetrics(inst.Metrics with { LastSuccess = inst.Metrics.LastAttempt });
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).IsAccepted.Should().BeTrue();
	}
}
