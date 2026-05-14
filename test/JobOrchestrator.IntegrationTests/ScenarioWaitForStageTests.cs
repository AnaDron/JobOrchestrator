using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// E2E-тесты для <see cref="IJobOrchestrator.WaitForStageSuccessAsync"/> и
/// <see cref="IJobOrchestrator.WaitForStageOutcomeAsync"/> — главные public-API фичи переноса.
/// </summary>
public sealed class ScenarioWaitForStageTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task WaitForStageSuccess_KeylessStage_ResolvesAfterFirstSuccess() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var waitTask = orchestrator.WaitForStageSuccessAsync("a");
			await waitTask.WaitAsync(Timeout).ConfigureAwait(false);
			fake.CallCount.Should().BeGreaterThan(0);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_AlreadySucceeded_ResolvesImmediately() {
		// Memoization: register-after-success получает completed Task сразу.
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// Дать event loop'у обработать StageCompleted.
			await Task.Delay(100).ConfigureAwait(false);

			var alreadyCompleted = orchestrator.WaitForStageSuccessAsync("a");
			alreadyCompleted.IsCompletedSuccessfully.Should().BeTrue(
				"fast-path: LastSuccess уже выставлен → SignalSuccess сразу резолвит");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageOutcome_Success_ReturnsSuccessKind() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var outcomeTask = orchestrator.WaitForStageOutcomeAsync("a");
			var outcome = await outcomeTask.WaitAsync(Timeout).ConfigureAwait(false);
			outcome.Kind.Should().Be(StageOutcomeKind.Success);
			outcome.Exception.Should().BeNull();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageOutcome_Failure_ReturnsFailureKindWithException() {
		var fake = new FakeServiceA();
		fake.ExecuteHandler = (_, _) => throw new InvalidOperationException("planned failure");

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromHours(1)))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var outcome = await orchestrator.WaitForStageOutcomeAsync("a").WaitAsync(Timeout).ConfigureAwait(false);
			outcome.Kind.Should().Be(StageOutcomeKind.Failure);
			outcome.Exception.Should().NotBeNull();
			outcome.Exception!.Message.Should().Contain("planned failure");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_InstanceCascaded_ResolvesWithInvalidOperationException() {
		// shops эмитит ключ → создаёт pg[shops=u1]. Затем UnregisterKey удаляет pg[shops=u1] до того,
		// как он успел отработать (он сразу запускается, но мы быстро удалим). WaitForStageSuccessAsync
		// для pg[shops=u1] должен throw InvalidOperationException через каскадную отмену.
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = (ctx, _) => { ctx.AddKey("u1"); return Task.CompletedTask; };
		var pgFake = new FakeServiceB();
		// pg висит долго — гарантирует, что мы успеем UnregisterKey до StageCompleted.
		pgFake.ExecuteHandler = async (_, ct) => {
			try { await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false); }
			catch (OperationCanceledException) { throw; }
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("pg должен начать выполняться");

			var waitTask = orchestrator.WaitForStageSuccessAsync("pg",
				new Dictionary<string, string>(StringComparer.Ordinal) { ["shops"] = "u1" });
			orchestrator.UnregisterKey("shops", "u1");

			Func<Task> awaitWaiter = async () => await waitTask.WaitAsync(Timeout).ConfigureAwait(false);
			(await awaitWaiter.Should().ThrowAsync<InvalidOperationException>().ConfigureAwait(false))
				.WithMessage("*удалён каскадом*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_UnknownStage_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			Func<Task> act = async () => await orchestrator.WaitForStageSuccessAsync("nonexistent").ConfigureAwait(false);
			await act.Should().ThrowAsync<ArgumentException>().ConfigureAwait(false);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_InvalidKeys_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			Func<Task> act = async () => await orchestrator.WaitForStageSuccessAsync("a",
				new Dictionary<string, string>(StringComparer.Ordinal) { ["unexpected"] = "x" }).ConfigureAwait(false);
			await act.Should().ThrowAsync<ArgumentException>().ConfigureAwait(false);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_CancellationToken_CancelsWaiterWithoutLeakingPending() {
		var fake = new FakeServiceA();
		// Stage блокируется надолго (чтобы success НЕ случился до cancel).
		fake.ExecuteHandler = async (_, ct) => {
			try { await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false); } catch (OperationCanceledException) { throw; }
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			using var cts = new CancellationTokenSource();
			var waitTask = orchestrator.WaitForStageSuccessAsync("a", null, cts.Token);
			cts.Cancel();
			Func<Task> act = async () => await waitTask.ConfigureAwait(false);
			await act.Should().ThrowAsync<OperationCanceledException>().ConfigureAwait(false);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
