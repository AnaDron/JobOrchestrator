using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Сценарии handle-API, которые мутируют state оркестратора (RunAsync, RegisterKey,
/// эмиссия ключей, ожидание iteration). Каждый тест строит свой <see cref="IHost"/> —
/// shared-fixture здесь привёл бы к cross-test interference.
/// <para>
/// Read-only сценарии (Indexer-валидация, identity-equality, и т. п.) живут в
/// <see cref="ScenarioHandleApiReadonlyTests"/> с разделяемыми class-fixture'ами.
/// </para>
/// </summary>
public sealed class ScenarioHandleApiTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task InstanceKey_None_KeylessRunAsync_Works() {
		// orchestrator.Root["a"][InstanceKeys.Empty].RunAsync() — keyless через handle-API.
		using var host = HandleApiTestHelpers.BuildKeylessAHost();
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("auto-tick");
			await HandleApiTestHelpers.WaitForFirstIdleAsync(orchestrator.Root["a"][InstanceKeys.Empty], Timeout)
				.ConfigureAwait(false);

			var iteration = await orchestrator.Root["a"][InstanceKeys.Empty].RunAsync().ConfigureAwait(false);
			iteration.Should().NotBeNull();
			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task SingleKey_Indexer_Works() {
		// orchestrator.Root["pg"][("shops","u1")].RunAsync() — 1-key через tuple-indexer.
		using var host = HandleApiTestHelpers.BuildShopsPgHost();
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await HandleApiTestHelpers.WaitForFirstIdleAsync(orchestrator.Root["pg"][("shops", "u1")], Timeout)
				.ConfigureAwait(false);

			var iteration = await orchestrator.Root["pg"][("shops", "u1")].RunAsync().ConfigureAwait(false);
			iteration.Should().NotBeNull();
			(await pgFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task State_KeylessInstance_ReflectsLifecycle() {
		using var host = HandleApiTestHelpers.BuildKeylessAHost();
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			var handle = orchestrator.Root["a"][InstanceKeys.Empty];
			await HandleApiTestHelpers.WaitForFirstIdleAsync(handle, Timeout).ConfigureAwait(false);

			handle.State.Should().Be(InstanceLifecycleState.Idle, "после first iteration инстанс Idle");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RegisterKey_OnStageHandle_PropagatesToCascade() {
		using var host = HandleApiTestHelpers.BuildShopsPgHost();
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();

			// External bootstrap через handle-API.
			orchestrator.Root["shops"].RegisterKey("u-ext");
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("pg-инстанс с emitted key");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForSuccessAsync_OnInstanceHandle_Resolves() {
		using var host = HandleApiTestHelpers.BuildKeylessAHost();
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			// Гарантируем, что bootstrap-итерация прошла и snapshot.LastSuccess зафиксирован,
			// чтобы WaitForSuccessAsync ушёл по fast-path (Phase 1), а не по timing-зависимым
			// Phase 3 (running iteration) / Phase 4 (stream loop) — это устраняет race с
			// DueScanner-тиком, который раньше делал тест flaky под нагрузкой test-host'а.
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await HandleApiTestHelpers.WaitForFirstIdleAsync(orchestrator.Root["a"][InstanceKeys.Empty], Timeout)
				.ConfigureAwait(false);

			await orchestrator.Root["a"][InstanceKeys.Empty]
				.WaitForSuccessAsync(new CancellationTokenSource(Timeout).Token)
				.ConfigureAwait(false);
			// Достижение этой точки — success.
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task AllInstances_AfterEmitting_ReflectsAllEmittedKeys() {
		using var host = HandleApiTestHelpers.BuildShopsPgHost();
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
			await ctx.AddKeyAsync("u2", ct);
			await ctx.AddKeyAsync("u3", ct);
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(3, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// Все три pg-инстанса должны достичь Idle (исходный тест полагался на Task.Delay(100)).
			foreach (var key in new[] { "u1", "u2", "u3" }) {
				await HandleApiTestHelpers.WaitForFirstIdleAsync(orchestrator.Root["pg"][("shops", key)], Timeout)
					.ConfigureAwait(false);
			}

			var instances = orchestrator.Root["pg"];
			instances.Should().HaveCount(3);
			instances.Select(i => i.Keys["shops"]).Should().BeEquivalentTo(["u1", "u2", "u3"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task IJobOrchestrator_AllStages_ReturnsFlatStageList() {
		// foreach (var stage in orchestrator) теперь даёт IDomainHandle. Плоский срез по стадиям —
		// через extension orchestrator.AllStages() либо orchestrator.Root для бездоменных.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("b").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var names = orchestrator.AllStages().Select(s => s.Name).OrderBy(n => n).ToArray();
			names.Should().BeEquivalentTo(["a", "b"]);
			// foreach по orchestrator — это домены; для бездоменных только root.
			orchestrator.Should().HaveCount(1, because: "только root-домен (нет .WithDomain)");
			orchestrator.Root.Select(s => s.Name).Should().BeEquivalentTo(["a", "b"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
