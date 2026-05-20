namespace JobOrchestrator.Tests.Internal;

public sealed class OutcomeWaitersTests {
	private static InstanceIdentity MakeIdentity() =>
		new(TestStages.Make("x"));

	[Fact]
	public async Task Register_AfterSignal_WaitsForNextOutcome() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();

		waiters.Signal(identity, StageOutcome.Success);

		var task = waiters.Register(identity, CancellationToken.None);
		task.IsCompleted.Should().BeFalse("прошлый исход не мемоизируется — ждём следующий Signal");
		waiters.Signal(identity, StageOutcome.FromFailure(new InvalidOperationException("second")));
		(await task.ConfigureAwait(false)).Kind.Should().Be(StageOutcomeKind.Failure);
	}

	[Fact]
	public async Task Signal_WithPendingSubscriber_ResolvesOnlyThatGeneration() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();
		var pending = waiters.Register(identity, CancellationToken.None);

		waiters.Signal(identity, StageOutcome.Success);

		(await pending.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false)).Kind
			.Should().Be(StageOutcomeKind.Success);

		var second = waiters.Register(identity, CancellationToken.None);
		second.IsCompleted.Should().BeFalse();
		waiters.Signal(identity, StageOutcome.FromCancellation(new InvalidOperationException("c")));
		(await second.ConfigureAwait(false)).Kind.Should().Be(StageOutcomeKind.Cancelled);
	}

	[Fact]
	public async Task Register_BeforeSignal_ResolvesOnSignal() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();
		var pending = waiters.Register(identity, CancellationToken.None);

		waiters.Signal(identity, StageOutcome.Success);

		(await pending.ConfigureAwait(false)).Kind.Should().Be(StageOutcomeKind.Success);
	}

	[Fact]
	public void Reset_ClearsBucket_NewRegisterWaitsFresh() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();
		waiters.Signal(identity, StageOutcome.Success);
		waiters.Reset(identity);

		var task = waiters.Register(identity, CancellationToken.None);
		task.IsCompleted.Should().BeFalse();
	}

	[Fact]
	public async Task FailAll_ThrowsOnPendingAndBlocksNewRegister() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();
		var pending = waiters.Register(identity, CancellationToken.None);
		var ex = new InvalidOperationException("stopped");

		waiters.FailAll(ex);

		Func<Task> awaitPending = async () => await pending.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
		await awaitPending.Should().ThrowAsync<InvalidOperationException>().ConfigureAwait(false);
		Func<Task> registerAfterStop = () => waiters.Register(identity, CancellationToken.None);
		await registerAfterStop.Should().ThrowAsync<InvalidOperationException>().ConfigureAwait(false);
	}
}
