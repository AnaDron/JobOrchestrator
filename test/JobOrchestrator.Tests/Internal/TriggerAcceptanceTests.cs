namespace JobOrchestrator.Tests.Internal;

public sealed class TriggerAcceptanceTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(TimeSpan debounce, RetryPolicy retry) => new() {
		Name = "x",
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = retry,
		Debounce = debounce,
		Dependencies = [],
		InstanceKeyNames = [],
	};

	private static StageInstance MakeInstance(StageDescriptor stage) => new() {
		Stage = stage,
		DependencyKeys = new Dictionary<string, string>(),
		FullyQualifiedName = "x[]",
		EncodedKey = "",
	};

	private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

	[Fact]
	public void Running_AlwaysRejectedWithAlreadyRunning() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		inst.State = InstanceLifecycleState.Running;
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Should().Be(TriggerResult.AlreadyRunning);
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.AlreadyRunning);
	}

	[Fact]
	public void Idle_NoHistory_AutoAndManualAccepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Auto_InRetryDelay_ReturnsWaitingRetry() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.ConsecutiveFailures = 1;
		inst.LastAttempt = Now.AddMinutes(-2);   // ещё в окне retry-delay (5 мин)
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Should().Be(TriggerResult.WaitingRetry);
	}

	[Fact]
	public void Auto_RetryDelayElapsed_Accepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.ConsecutiveFailures = 1;
		inst.LastAttempt = Now.AddMinutes(-10);  // окно прошло
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Manual_IgnoresRetryDelay() {
		var inst = MakeInstance(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		inst.ConsecutiveFailures = 5;
		inst.LastAttempt = Now.AddSeconds(-1);    // явно в окне retry-delay
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterSuccess_Rejected() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.LastAttempt = Now.AddSeconds(-5);
		inst.LastSuccess = inst.LastAttempt;
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterFailure_AlsoRejected() {
		// Debounce работает от LastAttempt вне зависимости от исхода последней попытки.
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.LastAttempt = Now.AddSeconds(-5);
		inst.ConsecutiveFailures = 1;
		inst.LastError = "boom";
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_DebounceElapsed_Accepted() {
		var inst = MakeInstance(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		inst.LastAttempt = Now.AddSeconds(-30);
		TriggerAcceptance.TryAccept(inst, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Auto_DebounceNotChecked() {
		// Auto не проверяет debounce — scheduled tick через interval сам по себе соблюдает темп.
		var inst = MakeInstance(MakeStage(TimeSpan.FromMinutes(1), RetryPolicy.NoRetry));
		inst.LastAttempt = Now.AddSeconds(-1);
		inst.LastSuccess = inst.LastAttempt;
		TriggerAcceptance.TryAccept(inst, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
	}
}
