using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioManualTriggerTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task Indexer_UnknownStage_ThrowsArgumentException() {
		// Handle-API даёт более точную семантику чем старый flat TriggerAsync("nonexistent"):
		// stage-name резолвится в индексаторе → unknown-stage = ArgumentException на construction,
		// а не TriggerResult.NotFound на execution.
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
	public async Task TriggerAsync_AfterFailureInRetryDelay_ManualIgnoresIt() {
		// Конфиг: длинный retry-delay; вызываем итерацию, она падает; сразу же делаем Manual trigger.
		// Auto был бы WaitingRetry, Manual должен принять.
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
			// Дать event loop'у обработать StageFailed.
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var result = await orchestrator["a"][InstanceKey.None].TriggerAsync().ConfigureAwait(false);
			result.Should().Be(TriggerResult.Started, "Manual игнорирует retry-delay");

			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task TriggerAsync_InheritedKeyThroughDependsOn_IsAcceptedNotInvalidKeys() {
		// Regression-тест на bug в плоском InstanceKeyNames:
		// products нет прямого DependsOnInstance(shops), но через DependsOn(productGroups) измерение
		// `shops` унаследовано. TriggerAsync("products", { shops: "u1" }) должен принять keys.
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = (ctx, _) => { ctx.AddKey("u1"); return Task.CompletedTask; };

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
			// Manual triggered c унаследованным ключом — handle-API валидирует key-names против
			// ExpectedKeyNames стадии (которая включает транзитивные через DependsOn → у products
			// есть `shops`-измерение из productGroups).
			var result = await orchestrator["products"][("shops", "u1")].TriggerAsync().ConfigureAwait(false);
			result.Should().BeOneOf(TriggerResult.Started, TriggerResult.Debounced, TriggerResult.AlreadyRunning);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_KeylessWithUnexpectedKey_ThrowsArgumentException() {
		// Keyless-стадия + попытка передать key → fail-fast в handle-construction (handle-API
		// заменил runtime InvalidKeys-семантику на compile-time-like ArgumentException).
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
	public async Task TriggerAsync_WhenConcurrencyLimitExhausted_ReturnsWaitingRetryNotStarted() {
		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = (ctx, _) => {
			for (int i = 0; i < 3; i++) ctx.AddKey($"shop-{i}");
			return Task.CompletedTask;
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
			var result = await orchestrator["productGroups"][("shops", idleShop)].TriggerAsync().ConfigureAwait(false);
			result.Should().Be(TriggerResult.WaitingRetry,
				"Manual trigger при занятом ConcurrencyLimit не должен возвращать Started до TryBeginRunning");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task TriggerAsync_InDebounceWindow_ReturnsDebounced() {
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
			// Дать event loop'у обработать StageCompleted — выставить LastAttempt.
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var result = await orchestrator["a"][InstanceKey.None].TriggerAsync().ConfigureAwait(false);
			result.Should().Be(TriggerResult.Debounced);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
