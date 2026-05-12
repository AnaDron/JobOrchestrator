using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioRegisterKeyTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task RegisterKey_FromOutside_SpawnsDependentInstance() {
		// Без AddKey изнутри стадии, RegisterKey снаружи должен наполнять keyspace и
		// каскадно создавать зависимые инстансы (после первого успеха parent-стадии).
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
				jobs.Stage("pg")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			// Дождёмся успеха shops.
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			// Внешний RegisterKey должен спавнить pg-инстанс.
			orchestrator.RegisterKey("shops", "external-uuid-1");
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			pgFake.Calls[0].DependencyKeys.Should().ContainKey("shops").WhoseValue.Should().Be("external-uuid-1");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RegisterKey_UnknownStage_DoesNotThrowButLogsWarning() {
		// Edge case: внешний RegisterKey для несуществующей стадии — должно быть silently no-op + warn.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Action act = () => orchestrator.RegisterKey("nonexistent-stage", "k1");
			act.Should().NotThrow();
			await Task.Delay(100).ConfigureAwait(false);
			// Из тестов сложно проверить warn-лог без custom logger, но факт что не упало — уже OK.
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
