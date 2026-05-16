using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// End-to-end проверка domain-API: builder-side prefix + consumer-side
/// <see cref="IJobOrchestrator.WithDomain"/> работают согласованно.
/// </summary>
public sealed class ScenarioDomainApiTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task WithDomain_OrchestratorConsumerSide_ResolvesStage() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var fake = host.Services.GetRequiredService<FakeServiceA>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();

			// Эквивалентность двух путей доступа.
			var viaFullName = orchestrator["evotor:shops"];
			var viaDomain = orchestrator.WithDomain("evotor")["shops"];
			viaDomain.Should().BeSameAs(viaFullName,
				because: "domain-проекция делегирует в тот же кэшированный StageHandle");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public void WithDomain_UnknownStage_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		Action act = () => _ = orchestrator.WithDomain("evotor")["nonexistent"];
		act.Should().Throw<ArgumentException>(because: "evotor:nonexistent не зарегистрирован в графе");
	}

	[Fact]
	public void WithDomain_UnknownDomain_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		Action act = () => _ = orchestrator.WithDomain("ozon");
		act.Should().Throw<ArgumentException>(
			because: "домен 'ozon' не зарегистрирован — ни одной стадии с префиксом 'ozon:' нет");
	}

	[Fact]
	public void WithDomain_CachedPerDomain() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		var first = orchestrator.WithDomain("evotor");
		var second = orchestrator.WithDomain("evotor");
		first.Should().BeSameAs(second, because: "ConcurrentDictionary.GetOrAdd кэширует per-domain");
	}
}
