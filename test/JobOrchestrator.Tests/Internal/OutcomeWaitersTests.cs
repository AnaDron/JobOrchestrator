namespace JobOrchestrator.Tests.Internal;

public sealed class OutcomeWaitersTests {
	private static InstanceIdentity MakeIdentity() =>
		new(TestStages.Make("x"));

	[Fact]
	public void Signal_WithoutSubscribers_DoesNotCreateBucket() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();

		waiters.Signal(identity, StageOutcome.Success);

		var task = waiters.Register(identity, CancellationToken.None);
		task.IsCompleted.Should().BeFalse();
	}

	[Fact]
	public async Task Signal_WithPendingSubscriber_ResolvesAndMemoizesForLateRegister() {
		var waiters = new OutcomeWaiters();
		var identity = MakeIdentity();
		var pending = waiters.Register(identity, CancellationToken.None);

		waiters.Signal(identity, StageOutcome.Success);

		(await pending.WaitAsync(TimeSpan.FromSeconds(1)).ConfigureAwait(false)).Kind
			.Should().Be(StageOutcomeKind.Success);

		var late = await waiters.Register(identity, CancellationToken.None).ConfigureAwait(false);
		late.Kind.Should().Be(StageOutcomeKind.Success);
	}
}
