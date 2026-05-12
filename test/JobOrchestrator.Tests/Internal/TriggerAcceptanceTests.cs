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
	};

	private static Job MakeJob(StageDescriptor stage) => new() {
		Stage = stage,
		DependencyKeys = new Dictionary<string, string>(),
		FullyQualifiedName = "x[]",
		EncodedKey = "",
	};

	private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

	[Fact]
	public void Running_AlwaysRejectedWithAlreadyRunning() {
		var job = MakeJob(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		job.State = JobLifecycleState.Running;
		TriggerAcceptance.TryAccept(job, TriggerSource.Auto, Now).Should().Be(TriggerResult.AlreadyRunning);
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.AlreadyRunning);
	}

	[Fact]
	public void Idle_NoHistory_AutoAndManualAccepted() {
		var job = MakeJob(MakeStage(TimeSpan.Zero, RetryPolicy.NoRetry));
		TriggerAcceptance.TryAccept(job, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Auto_InRetryDelay_ReturnsWaitingRetry() {
		var job = MakeJob(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		job.ConsecutiveFailures = 1;
		job.LastAttempt = Now.AddMinutes(-2);   // ещё в окне retry-delay (5 мин)
		TriggerAcceptance.TryAccept(job, TriggerSource.Auto, Now).Should().Be(TriggerResult.WaitingRetry);
	}

	[Fact]
	public void Auto_RetryDelayElapsed_Accepted() {
		var job = MakeJob(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		job.ConsecutiveFailures = 1;
		job.LastAttempt = Now.AddMinutes(-10);  // окно прошло
		TriggerAcceptance.TryAccept(job, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Manual_IgnoresRetryDelay() {
		var job = MakeJob(MakeStage(TimeSpan.Zero, RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))));
		job.ConsecutiveFailures = 5;
		job.LastAttempt = Now.AddSeconds(-1);    // явно в окне retry-delay
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterSuccess_Rejected() {
		var job = MakeJob(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		job.LastAttempt = Now.AddSeconds(-5);
		job.LastSuccess = job.LastAttempt;
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_InDebounceWindow_AfterFailure_AlsoRejected() {
		// Debounce работает от LastAttempt вне зависимости от исхода последней попытки.
		var job = MakeJob(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		job.LastAttempt = Now.AddSeconds(-5);
		job.ConsecutiveFailures = 1;
		job.LastError = "boom";
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.Debounced);
	}

	[Fact]
	public void Manual_DebounceElapsed_Accepted() {
		var job = MakeJob(MakeStage(TimeSpan.FromSeconds(10), RetryPolicy.NoRetry));
		job.LastAttempt = Now.AddSeconds(-30);
		TriggerAcceptance.TryAccept(job, TriggerSource.Manual, Now).Should().Be(TriggerResult.Started);
	}

	[Fact]
	public void Auto_DebounceNotChecked() {
		// Auto не проверяет debounce — scheduled tick через interval сам по себе соблюдает темп.
		var job = MakeJob(MakeStage(TimeSpan.FromMinutes(1), RetryPolicy.NoRetry));
		job.LastAttempt = Now.AddSeconds(-1);
		job.LastSuccess = job.LastAttempt;
		TriggerAcceptance.TryAccept(job, TriggerSource.Auto, Now).Should().Be(TriggerResult.Started);
	}
}
