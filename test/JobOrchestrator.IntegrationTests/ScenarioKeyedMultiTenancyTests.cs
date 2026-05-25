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
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("ozon", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("ozon", ozon =>
					ozon.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			// Keyed-singleton: overrides probe-pass TryAddKeyedScoped, чтобы test и runner видели один инстанс.
			s.AddKeyedSingleton<FakeServiceA>("evotor", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("ozon", new FakeServiceB());
		});

		var evotor = host.Services.GetRequiredKeyedService<IJobOrchestrator>("evotor");
		var ozon = host.Services.GetRequiredKeyedService<IJobOrchestrator>("ozon");

		evotor.Should().NotBeSameAs(ozon);

		// Каждый видит ТОЛЬКО свои стадии.
		evotor.Invoking(o => _ = o.GetStage("evotor:shops")).Should().NotThrow();
		evotor.Invoking(o => _ = o.GetStage("ozon:offers")).Should().Throw<ArgumentException>(
			because: "стадия ozon:offers зарегистрирована под другим tenant'ом");
		ozon.Invoking(o => _ = o.GetStage("ozon:offers")).Should().NotThrow();
		ozon.Invoking(o => _ = o.GetStage("evotor:shops")).Should().Throw<ArgumentException>(
			because: "стадия evotor:shops зарегистрирована под другим tenant'ом");

		// Internal state — раздельный.
		host.Services.GetRequiredKeyedService<StageRegistry>("evotor")
			.Should().NotBeSameAs(host.Services.GetRequiredKeyedService<StageRegistry>("ozon"));
		host.Services.GetRequiredKeyedService<JobOrchestratorRuntime>("evotor")
			.Should().NotBeSameAs(host.Services.GetRequiredKeyedService<JobOrchestratorRuntime>("ozon"));
	}

	[Fact]
	public async Task KeyedMultiTenancy_BothOrchestratorsRunEndToEnd() {
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("ozon", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("ozon", ozon =>
					ozon.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton<FakeServiceA>("evotor", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("ozon", new FakeServiceB());
		});

		var evotorFake = host.Services.GetRequiredKeyedService<FakeServiceA>("evotor");
		var ozonFake = host.Services.GetRequiredKeyedService<FakeServiceB>("ozon");
		await host.StartAsync().ConfigureAwait(false);
		try {
			// Оба HostedService стартовали, оба event-loop'а выполняют свои стадии независимо.
			(await evotorFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			(await ozonFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			evotorFake.Calls[0].FullyQualifiedName.Should().Be("evotor:shops[]");
			ozonFake.Calls[0].FullyQualifiedName.Should().Be("ozon:offers[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public void KeyedMultiTenancy_MultipleAddJobOrchestratorPerTenant_AccumulateInSameTenant() {
		// Два вызова с tenantKey="evotor" — должны объединиться в один tenant'ный registry.
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("products").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton<FakeServiceA>("evotor", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("evotor", new FakeServiceB());
		});

		var registry = host.Services.GetRequiredKeyedService<StageRegistry>("evotor");
		registry.AllStages.Select(s => s.Name).Should()
			.BeEquivalentTo(["evotor:shops", "evotor:products"]);
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
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("shops").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddSingleton<FakeServiceA>();    // unkeyed для unkeyed-orchestrator
			s.AddKeyedSingleton<FakeServiceB>("evotor", new FakeServiceB());
		});

		var globalOrch = host.Services.GetRequiredService<IJobOrchestrator>();
		var evotorOrch = host.Services.GetRequiredKeyedService<IJobOrchestrator>("evotor");

		globalOrch.Should().NotBeSameAs(evotorOrch);
		globalOrch.Invoking(o => _ = o.GetStage("global")).Should().NotThrow();
		evotorOrch.Invoking(o => _ = o.GetStage("evotor:shops")).Should().NotThrow();
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
		var evotorInstance = new SharedStage();
		var ozonInstance = new SharedStage();
		using var host = BuildHost(s => {
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("work").HandledBy<SharedStage>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("ozon", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("ozon", ozon =>
					ozon.Stage("work").HandledBy<SharedStage>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton("evotor", evotorInstance);
			s.AddKeyedSingleton("ozon", ozonInstance);
		});

		await host.StartAsync().ConfigureAwait(false);
		try {
			var deadline = DateTime.UtcNow + Timeout;
			while (DateTime.UtcNow < deadline && (evotorInstance.CallCount == 0 || ozonInstance.CallCount == 0)) {
				await Task.Delay(50).ConfigureAwait(false);
			}
			evotorInstance.CallCount.Should().BeGreaterThan(0, because: "evotor:work должен был выполниться");
			ozonInstance.CallCount.Should().BeGreaterThan(0, because: "ozon:work должен был выполниться");
			// Два РАЗНЫХ инстанса — keyed-isolation работает на уровне DI.
			evotorInstance.Should().NotBeSameAs(ozonInstance);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
