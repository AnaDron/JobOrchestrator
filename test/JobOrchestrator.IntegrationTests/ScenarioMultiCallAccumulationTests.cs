using JobOrchestrator.IntegrationTests.Support;
using JobOrchestrator.Internal;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Тесты multi-call accumulation: <see cref="ServiceCollectionExtensions.AddJobOrchestrator"/>
/// можно вызывать многократно, configure-actions аккумулируются в один shared builder.
/// </summary>
public sealed class ScenarioMultiCallAccumulationTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	private static IHost BuildHost(Action<IServiceCollection> register) {
		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();
		register(builder.Services);
		return builder.Build();
	}

	[Fact]
	public void MultipleAddJobOrchestrator_AccumulateIntoSingleRegistry() {
		using var host = BuildHost(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddSingleton<FakeServiceA>();
			s.AddSingleton<FakeServiceB>();
		});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		orchestrator.Invoking(o => _ = o.GetStage("catalog:shops")).Should().NotThrow();
		orchestrator.Invoking(o => _ = o.GetStage("marketplace:offers")).Should().NotThrow();

		var registry = host.Services.GetRequiredService<StageRegistry>();
		registry.AllStages.Select(s => s.Name).Should().BeEquivalentTo(["catalog:shops", "marketplace:offers"]);
	}

	[Fact]
	public async Task MultipleAddJobOrchestrator_RunsEndToEnd() {
		using var host = BuildHost(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddSingleton<FakeServiceA>();
			s.AddSingleton<FakeServiceB>();
		});

		var catalogFake = host.Services.GetRequiredService<FakeServiceA>();
		var marketplaceFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			// Обе стадии — keyless, обе стартуют на bootstrap, обе вызывают свой IJobService.
			(await catalogFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			(await marketplaceFake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			catalogFake.Calls[0].FullyQualifiedName.Should().Be("catalog:shops[]");
			marketplaceFake.Calls[0].FullyQualifiedName.Should().Be("marketplace:offers[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public void MultipleAddJobOrchestrator_OrderIndependent() {
		// Те же два модуля, но в обратном порядке регистрации — итоговый набор стадий идентичен.
		using var host = BuildHost(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("marketplace", marketplace =>
					marketplace.Stage("offers").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("catalog", catalog =>
					catalog.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddSingleton<FakeServiceA>();
			s.AddSingleton<FakeServiceB>();
		});

		var registry = host.Services.GetRequiredService<StageRegistry>();
		registry.AllStages.Select(s => s.Name).Should().BeEquivalentTo(["catalog:shops", "marketplace:offers"]);
	}

	[Fact]
	public void MultipleAddJobOrchestrator_DuplicateStageName_ThrowsAtFirstResolve() {
		// Два configure-action'а декларируют одно и то же имя — collision ловится на lazy-resolve
		// StageRegistry, а не на этапе AddJobOrchestrator (probe-pass работает per-call).
		using var host = BuildHost(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("shared").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1));
			});
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("shared").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1));
			});
			s.AddSingleton<FakeServiceA>();
			s.AddSingleton<FakeServiceB>();
		});

		Action act = () => host.Services.GetRequiredService<StageRegistry>();
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*'shared'*");
	}

	[Fact]
	public void MultipleAddJobOrchestrator_StageServiceTypesRegisteredEagerly() {
		// IJobService-типы из всех configure-вызовов резолвятся через DI как scoped — это
		// должно произойти ДО первого resolve IJobOrchestrator (probe-pass eager-регистрирует).
		using var host = BuildHost(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1));
			});
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("b").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromMinutes(1));
			});
			// FakeServiceA/B не регистрируем явно — probe-pass должен сделать TryAddScoped автоматически.
		});

		using var scope = host.Services.CreateScope();
		scope.ServiceProvider.GetService<FakeServiceA>().Should().NotBeNull(
			because: "probe-pass для первого AddJobOrchestrator должен был TryAddScoped<FakeServiceA>");
		scope.ServiceProvider.GetService<FakeServiceB>().Should().NotBeNull(
			because: "probe-pass для второго AddJobOrchestrator должен был TryAddScoped<FakeServiceB>");
	}
}
