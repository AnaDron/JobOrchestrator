using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioManualTriggerTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task Indexer_UnknownStage_ThrowsArgumentException() {
		// Handle-API: unknown-stage = ArgumentException на construction индексатора,
		// не runtime-reject в RunAsync.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Action act = () => _ = orchestrator["nonexistent"];
			act.Should().Throw<ArgumentException>().WithMessage("*nonexistent*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_AfterFailureInRetryDelay_ManualIgnoresIt() {
		// Конфиг: длинный retry-delay; первая итерация падает; сразу же Manual RunAsync.
		// Auto был бы WaitingRetry, Manual принимает.
		var fake = new FakeServiceA();
		int calls = 0;
		fake.ExecuteHandler = (ctx, _) => {
			calls++;
			if (calls == 1) throw new InvalidOperationException("first call fails");
			return Task.CompletedTask;
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromMinutes(10)))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var iteration = await orchestrator["a"][InstanceKey.None].RunAsync().WaitAsync(Timeout).ConfigureAwait(false);
			// Manual игнорирует retry-delay; second call успешен → тихий await без exception.
			await iteration.Completion.WaitAsync(Timeout).ConfigureAwait(false);

			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_InheritedKeyThroughDependsOn_IsAcceptedNotRejected() {
		// products: нет прямого DependsOnInstance(shops), но через DependsOn(productGroups) измерение
		// `shops` унаследовано. RunAsync("products", { shops: "u1" }) должен принять keys без
		// ArgumentException на construction индексатора.
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				var pg = jobs.Stage("productGroups").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("products").HandledBy<FakeServiceC>().DependsOn(pg).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>();
				s.AddSingleton<FakeServiceC>();
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			var productsFake = host.Services.GetRequiredService<FakeServiceC>();
			(await productsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false))
				.Should().BeTrue("products[shops=u1] должен пробуститься после каскада");

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			// handle-API валидирует key-names против ExpectedKeyNames — должно пройти construction
			// без ArgumentException. Runtime может отвергнуть AlreadyRunning/Debounced — это OK,
			// ключевой инвариант: keys приняты handle-API.
			try {
				_ = await orchestrator["products"][("shops", "u1")].RunAsync().ConfigureAwait(false);
			} catch (IterationRejectedException ex) {
				ex.Reason.Should().BeOneOf(IterationRejectReason.AlreadyRunning, IterationRejectReason.Debounced);
			}
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_KeylessWithUnexpectedKey_ThrowsArgumentException() {
		// Keyless-стадия + попытка передать key → fail-fast в handle-construction:
		// ArgumentException на construction, без runtime-проверок.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("keyless").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Action act = () => _ = orchestrator["keyless"][("unexpected", "x")];
			act.Should().Throw<ArgumentException>();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_WhenConcurrencyLimitExhausted_ThrowsConcurrencyDeferred() {
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			for (int i = 0; i < 3; i++) await ctx.AddKeyAsync($"shop-{i}", ct);
		};

		var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		string? runningShopKey = null;
		var pgFake = new FakeServiceB();
		pgFake.ExecuteHandler = async (ctx, ct) => {
			runningShopKey = ctx.DependencyKeys["shops"];
			gate.TrySetResult();
			await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false);
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.WithConcurrencyLimit(1)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await gate.Task.WaitAsync(Timeout).ConfigureAwait(false);
			runningShopKey.Should().NotBeNull();
			var idleShop = runningShopKey == "shop-0" ? "shop-1" : "shop-0";

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Func<Task> act = () => orchestrator["productGroups"][("shops", idleShop)].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.ConcurrencyDeferred,
				"Manual RunAsync при занятом ConcurrencyLimit отвергается до TryBeginRunning");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunAsync_InDebounceWindow_ThrowsDebounced() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.Debounce(TimeSpan.FromMinutes(10))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Func<Task> act = () => orchestrator["a"][InstanceKey.None].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.Debounced);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
