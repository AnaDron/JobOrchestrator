using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// End-to-end проверка domain-API: builder-side prefix + consumer-side <see cref="IJobOrchestrator.this[string]"/>
/// работают согласованно через трёхуровневую иерархию <see cref="IDomainHandle"/> → <see cref="IStageHandle"/>.
/// </summary>
public sealed class ScenarioDomainApiTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task DomainHandle_ResolvesStage_SameAsGetStage() {
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

			// Эквивалентность двух путей доступа: full-name через extension и domain-scope через индексатор.
			var viaFullName = orchestrator.GetStage("evotor:shops");
			var viaDomain = orchestrator["evotor"]["shops"];
			ReferenceEquals(viaDomain, viaFullName).Should().BeTrue(
				because: "StageHandle cached в Runtime — оба пути возвращают тот же singleton");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public void DomainHandle_UnknownStage_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		Action act = () => _ = orchestrator["evotor"]["nonexistent"];
		act.Should().Throw<ArgumentException>(because: "стадия 'nonexistent' не зарегистрирована в домене 'evotor'");
	}

	[Fact]
	public void Indexer_UnknownDomain_ThrowsArgumentException() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		Action act = () => _ = orchestrator["ozon"];
		act.Should().Throw<ArgumentException>(
			because: "домен 'ozon' не зарегистрирован — ни одной стадии с префиксом 'ozon:' нет");
	}

	[Fact]
	public void DomainHandle_CachedPerDomain() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.WithDomain("evotor", evotor => {
				evotor.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			}),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		var first = orchestrator["evotor"];
		var second = orchestrator["evotor"];
		ReferenceEquals(first, second).Should().BeTrue(
			because: "DomainHandle frozen on construction — повторные lookup'ы возвращают тот же объект");
	}

	[Fact]
	public void Root_EquivalentToEmptyDomainIndexer() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("flat").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		ReferenceEquals(orchestrator.Root, orchestrator[DomainName.Root]).Should().BeTrue(
			because: "Root ≡ this[DomainName.Root] — convenience-property");
		orchestrator.Root.Name.Should().Be(DomainName.Root);
	}
}
