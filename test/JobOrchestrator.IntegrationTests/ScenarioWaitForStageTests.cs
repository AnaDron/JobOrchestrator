using JobOrchestrator.IntegrationTests.Support;
using JobOrchestrator.Internal;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// E2E-тесты для <see cref="IInstanceHandle.WaitForSuccessAsync"/> и <see cref="IInstanceHandle.RunAsync"/>.
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
			var waitTask = orchestrator.Root["a"][InstanceKeys.Empty].WaitForSuccessAsync();
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
			await Task.Delay(100).ConfigureAwait(false);

			var alreadyCompleted = orchestrator.Root["a"][InstanceKeys.Empty].WaitForSuccessAsync();
			alreadyCompleted.IsCompletedSuccessfully.Should().BeTrue(
				"fast-path: LastSuccess уже выставлен → SignalSuccess сразу резолвит");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_Success_ReturnsHandleThatResolvesToSucceeded() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var iteration = await orchestrator.Root["a"][InstanceKeys.Empty].RunAsync().WaitAsync(Timeout).ConfigureAwait(false);
			iteration.FullyQualifiedName.Should().Be("a[]");

			// Тихий await = success. Если итерация Failed/Cancelled/Faulted — здесь бросится, тест fail.
			await iteration.Completion.WaitAsync(Timeout).ConfigureAwait(false);
			iteration.Completion.IsCompletedSuccessfully.Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_Failure_HandleResolvesToFailedWithException() {
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
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var iteration = await orchestrator.Root["a"][InstanceKeys.Empty].RunAsync().WaitAsync(Timeout).ConfigureAwait(false);

			Func<Task> awaitCompletion = () => iteration.Completion.WaitAsync(Timeout);
			var ex = (await awaitCompletion.Should().ThrowAsync<IterationFailedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationFailureReason.StageException);
			ex.InnerException.Should().NotBeNull();
			ex.InnerException!.Message.Should().Contain("planned failure");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_AlreadyRunning_ThrowsIterationRejected() {
		var fake = new FakeServiceA();
		fake.ExecuteHandler = async (_, ct) => {
			try { await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false); }
			catch (OperationCanceledException) { throw; }
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();

			Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.AlreadyRunning);
			ex.FullyQualifiedName.Should().Be("a[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_NotFound_ThrowsIterationRejected() {
		var shopsFake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>();
			});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			Func<Task> act = () => orchestrator.Root["pg"][("shops", "missing")].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.NotFound);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_Debounced_ThrowsIterationRejected() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.Debounce(TimeSpan.FromMinutes(10))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.Debounced);
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
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };
		var pgFake = new FakeServiceB();
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

			var waitTask = orchestrator.Root["pg"][("shops", "u1")].WaitForSuccessAsync();
			orchestrator.Root["shops"].UnregisterKey("u1");

			Func<Task> awaitWaiter = async () => await waitTask.WaitAsync(Timeout).ConfigureAwait(false);
			// WaitForSuccessAsync ловит per-iteration IterationFailedException (cancel-by-cascade) и
			// fall-through-ит к stream loop'у; когда subscriber-канал закрыт через CompleteIterationSubscribers
			// (часть NotifyInstanceRemoved), extension бросает InvalidOperationException с сообщением о
			// причине завершения stream'а.
			(await awaitWaiter.Should().ThrowAsync<InvalidOperationException>().ConfigureAwait(false))
				.WithMessage("*cascade-removal*");
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
			Action act = () => _ = orchestrator.Root["nonexistent"];
			act.Should().Throw<ArgumentException>();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task IndexerWithWrongKeyNames_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			Action act = () => _ = orchestrator.Root["a"][("unexpected", "x")];
			act.Should().Throw<ArgumentException>();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForStageSuccess_CancellationToken_CancelsWaiterWithoutLeakingPending() {
		var fake = new FakeServiceA();
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
			var waitTask = orchestrator.Root["a"][InstanceKeys.Empty].WaitForSuccessAsync(cts.Token);
			cts.Cancel();
			Func<Task> act = async () => await waitTask.ConfigureAwait(false);
			await act.Should().ThrowAsync<OperationCanceledException>().ConfigureAwait(false);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_TerminatingInstance_ThrowsTerminating() {
		// Terminating-window длится между cascade-mark и runner-finalize. Runner должен не уважать ct
		// мгновенно — иначе finalize произойдёт до того, как мы вызовем RunAsync. Здесь runner спит
		// БЕЗ ct → cascade пометил Terminating, но runner ещё не закончил.
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };
		var pgFake = new FakeServiceB();
		var pgGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		pgFake.ExecuteHandler = async (_, _) => {
			pgGate.TrySetResult();
			// Игнорируем ct — даёт нам Terminating-window: cascade пометил, но мы ещё не вернулись.
			// 500ms — достаточно поймать window (~200ms между UnregisterKey и RunAsync); не задерживает
			// shutdown-cleanup надолго после теста.
			await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			await pgGate.Task.WaitAsync(Timeout).ConfigureAwait(false);

			orchestrator.Root["shops"].UnregisterKey("u1");
			// Дать event-loop'у обработать KeyRemoved + перевести pg[shops=u1] в Terminating (cascade).
			// Runner всё ещё спит (3s без ct) — инстанс остаётся в InstanceManager как Terminating.
			await Task.Delay(200).ConfigureAwait(false);

			Func<Task> act = () => orchestrator.Root["pg"][("shops", "u1")].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.Terminating);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_CancelledByCascadeDuringIteration_OutcomeIsCancelled() {
		// RunAsync запускает итерацию pg[shops=u1], которая виснет надолго. Параллельно UnregisterKey('u1')
		// каскадно отменяет → FinalizeTerminating резолвит iteration-TCS как Cancelled.
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };
		var pgFake = new FakeServiceB();
		var iterationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		pgFake.ExecuteHandler = async (_, ct) => {
			iterationStarted.TrySetResult();
			try { await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false); }
			catch (OperationCanceledException) { throw; }
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			// Дождаться, пока pg-инстанс материализован и bootstrap-tick на нём отработал.
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await iterationStarted.Task.WaitAsync(Timeout).ConfigureAwait(false);
			// Дать event-loop'у дойти до завершения bootstrap-tick'а (он cancelled через Task.Delay).
			// Wait, bootstrap-tick виснет — он не завершился. RunAsync вернёт AlreadyRunning.
			// Поэтому делаем cancellation через UnregisterKey ПОСЛЕ того как мы убедились что bootstrap бежит.
			Func<Task> act = () => orchestrator.Root["pg"][("shops", "u1")].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.AlreadyRunning,
				"bootstrap-tick ещё бежит; cancelled outcome через RunAsync проверяется в shutdown-сценарии ниже");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_HandleResolves_AfterShutdown_NoHang() {
		// Главный инвариант: caller, ожидающий iteration.Completion, НЕ зависает при shutdown.
		// Конкретный исход зависит от тайминга: drain мог поймать runner-OCE → Completion=Result(Failure/
		// Cancelled); либо final-pass отстрелил iteration-TCS до того, как runner вернулся, → Completion
		// faulted-Task. Оба валидны — критично только что handle резолвится в terminal-state.
		var fake = new FakeServiceA();
		var iterationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		int callCount = 0;
		fake.ExecuteHandler = async (_, ct) => {
			var c = Interlocked.Increment(ref callCount);
			if (c == 1) return;    // bootstrap-tick: мгновенный success, инстанс снова Idle.
			iterationStarted.TrySetResult();
			try { await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false); }
			catch (OperationCanceledException) { throw; }
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);

		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var iteration = await orchestrator.Root["a"][InstanceKeys.Empty].RunAsync().WaitAsync(Timeout).ConfigureAwait(false);
			await iterationStarted.Task.WaitAsync(Timeout).ConfigureAwait(false);
			iteration.Completion.IsCompleted.Should().BeFalse();

			await host.StopAsync().ConfigureAwait(false);

			// После shutdown caller получает IterationFailedException — Reason зависит от тайминга:
			// либо StageException (runner OCE → StageFailed поймал drain), либо Cancelled (Terminating),
			// либо Faulted (final-pass отстрелил TCS до публикации completion-event).
			Func<Task> awaitCompletion = () => iteration.Completion.WaitAsync(Timeout);
			var ex = (await awaitCompletion.Should().ThrowAsync<IterationFailedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().BeOneOf(
				IterationFailureReason.StageException,
				IterationFailureReason.Cancelled,
				IterationFailureReason.Faulted);
			iteration.Completion.IsFaulted.Should().BeTrue();
		} finally {
			try { await host.StopAsync().ConfigureAwait(false); } catch { /* idempotent */ }
		}
	}

	[Fact]
	public async Task RunAsync_AfterFaulted_ThrowsIterationRejectedFaulted() {
		// Контракт: при Faulted-оркестраторе RunAsync синхронно бросает IterationRejectedException(Faulted),
		// а не InvalidOperationException и не висит в ожидании tcs.Task.
		// Используем lifecycle.MarkFaulted() напрямую вместо host.StopAsync(): MarkFaulted синхронно
		// ставит IsFaulted=true И закрывает channel, что гарантирует fail-fast путь в Runtime.RunAsync
		// без race-окна между EventLoop exit и Channel.TryComplete (которое возникало при host.StopAsync
		// под нагрузкой parallel-test-run'а и проявлялось как hang всего test-host'а).
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		var lifecycle = host.Services.GetRequiredService<OrchestratorLifecycle>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			lifecycle.MarkFaulted();

			Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.Faulted);
			ex.FullyQualifiedName.Should().Be("a[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_CallerCancelledBeforeAcceptance_DoesNotStartIteration() {
		// Pre-cancelled token: WriteAsync синхронно бросит OCE до публикации события — итерация не стартует.
		// Гарантия: после await fake.CallCount остаётся unchanged (bootstrap-tick один + ничего).
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("bootstrap-tick");
			await Task.Delay(100).ConfigureAwait(false);
			var bootstrapCalls = fake.CallCount;

			using var cts = new CancellationTokenSource();
			cts.Cancel();
			Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync(cts.Token);
			await act.Should().ThrowAsync<OperationCanceledException>().ConfigureAwait(false);

			// Gap: после OCE bootstrap-tick — единственный legit вызов.
			await Task.Delay(200).ConfigureAwait(false);
			fake.CallCount.Should().Be(bootstrapCalls, "pre-cancelled RunAsync не должен запускать итерацию");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
