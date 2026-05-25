using JobOrchestrator.IntegrationTests.Support;
using JobOrchestrator.Internal;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// End-to-end тесты keyed-services multi-tenancy (Path A): два независимых tenant'а под разными
/// ключами получают полностью изолированные StageRegistry/InstanceManager/Channel/EventLoop,
/// и стартуют оба HostedService параллельно.
/// </summary>
public sealed class ScenarioKeyedMultiTenancyTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	private static IHost BuildHost(Action<IServiceCollection> register) {
		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();
		register(builder.Services);
		return builder.Build();
	}

	[Fact]
	public void KeyedMultiTenancy_TwoTenants_HaveIsolatedRegistries() {
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("marketplace", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			// Keyed-singleton: overrides probe-pass TryAddKeyedScoped, чтобы test и runner видели один инстанс.
			s.AddKeyedSingleton<FakeServiceA>("catalog", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("marketplace", new FakeServiceB());
		});

		var catalog = host.Services.GetRequiredKeyedService<IJobOrchestrator>("catalog");
		var marketplace = host.Services.GetRequiredKeyedService<IJobOrchestrator>("marketplace");

		catalog.Should().NotBeSameAs(marketplace);

		// Каждый видит ТОЛЬКО свои стадии.
		catalog.Invoking(o => _ = o.GetStage("catalog:shops")).Should().NotThrow();
		catalog.Invoking(o => _ = o.GetStage("marketplace:offers")).Should().Throw<ArgumentException>(
			because: "стадия marketplace:offers зарегистрирована под другим tenant'ом");
		marketplace.Invoking(o => _ = o.GetStage("marketplace:offers")).Should().NotThrow();
		marketplace.Invoking(o => _ = o.GetStage("catalog:shops")).Should().Throw<ArgumentException>(
			because: "стадия catalog:shops зарегистрирована под другим tenant'ом");

		// Internal state — раздельный.
		host.Services.GetRequiredKeyedService<StageRegistry>("catalog")
			.Should().NotBeSameAs(host.Services.GetRequiredKeyedService<StageRegistry>("marketplace"));
		host.Services.GetRequiredKeyedService<InstanceManager>("catalog")
			.Should().NotBeSameAs(host.Services.GetRequiredKeyedService<InstanceManager>("marketplace"));
	}

	[Fact]
	public async Task KeyedMultiTenancy_BothOrchestratorsRunEndToEnd() {
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("marketplace", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton<FakeServiceA>("catalog", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("marketplace", new FakeServiceB());
		});

		var catalogFake = host.Services.GetRequiredKeyedService<FakeServiceA>("catalog");
		var marketplaceFake = host.Services.GetRequiredKeyedService<FakeServiceB>("marketplace");
		await host.StartAsync().ConfigureAwait(false);
		try {
			// Оба HostedService стартовали, оба event-loop'а выполняют свои стадии независимо.
			(await catalogFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			(await marketplaceFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			catalogFake.Calls[0].FullyQualifiedName.Should().Be("catalog:shops[]");
			marketplaceFake.Calls[0].FullyQualifiedName.Should().Be("marketplace:offers[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public void KeyedMultiTenancy_MultipleAddJobOrchestratorPerTenant_AccumulateInSameTenant() {
		// Два вызова с tenantKey="catalog" — должны объединиться в один tenant'ный registry.
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("products").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton<FakeServiceA>("catalog", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("catalog", new FakeServiceB());
		});

		var registry = host.Services.GetRequiredKeyedService<StageRegistry>("catalog");
		registry.AllStages.Select(s => s.Name).Should()
			.BeEquivalentTo(["catalog:shops", "catalog:products"]);
	}

	[Fact]
	public void UnkeyedAndKeyedCoexist() {
		using var host = BuildHost(s => {
			// Unkeyed (single-tenant) — стандартный сценарий.
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("global").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1));
			});
			// Keyed tenant в том же ServiceCollection — отдельный orchestrator.
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddSingleton<FakeServiceA>();    // unkeyed для unkeyed-orchestrator
			s.AddKeyedSingleton<FakeServiceB>("catalog", new FakeServiceB());
		});

		var globalOrch = host.Services.GetRequiredService<IJobOrchestrator>();
		var catalogOrch = host.Services.GetRequiredKeyedService<IJobOrchestrator>("catalog");

		globalOrch.Should().NotBeSameAs(catalogOrch);
		globalOrch.Invoking(o => _ = o.GetStage("global")).Should().NotThrow();
		catalogOrch.Invoking(o => _ = o.GetStage("catalog:shops")).Should().NotThrow();
	}

	private sealed class SharedStage : IJobService {
		public int CallCount;
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
			Interlocked.Increment(ref CallCount);
			return Task.CompletedTask;
		}
	}

	[Fact]
	public async Task KeyedMultiTenancy_SharedIJobServiceType_GetsKeyedScopedResolution() {
		// Два tenant'а используют ОДИН тип IJobService. Регистрируем как keyed-singleton per tenant —
		// DI выдаёт РАЗНЫЕ инстансы под разными ключами; каждый tenant дёргает свой.
		var catalogInstance = new SharedStage();
		var marketplaceInstance = new SharedStage();
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("catalog", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("work").HandledBy<SharedStage>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("marketplace", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("work").HandledBy<SharedStage>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton("catalog", catalogInstance);
			s.AddKeyedSingleton("marketplace", marketplaceInstance);
		});

		await host.StartAsync().ConfigureAwait(false);
		try {
			var deadline = DateTime.UtcNow + Timeout;
			while (DateTime.UtcNow < deadline && (catalogInstance.CallCount == 0 || marketplaceInstance.CallCount == 0)) {
				await Task.Delay(50).ConfigureAwait(false);
			}
			catalogInstance.CallCount.Should().BeGreaterThan(0, because: "catalog:work должен был выполниться");
			marketplaceInstance.CallCount.Should().BeGreaterThan(0, because: "marketplace:work должен был выполниться");
			// Два РАЗНЫХ инстанса — keyed-isolation работает на уровне DI.
			catalogInstance.Should().NotBeSameAs(marketplaceInstance);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
