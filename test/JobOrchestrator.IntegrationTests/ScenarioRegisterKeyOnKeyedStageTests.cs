using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// 3.2: внешний RegisterKey работает только для keyless-стадий. На ключевой стадии (с DependsOnInstance)
/// должен throw'ить InvalidOperationException с понятным сообщением.
/// </summary>
public sealed class ScenarioRegisterKeyOnKeyedStageTests {
	[Fact]
	public async Task RegisterKey_OnKeyedStage_ThrowsInvalidOperation() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			// productGroups имеет DependsOnInstance(shops) → у неё ExpectedKeyNames=[shops].
			// Внешний RegisterKey не может определить, какой инстанс productGroups использовать как Source.
			Action register = () => orchestrator.RegisterKey("productGroups", "anything");
			register.Should().Throw<InvalidOperationException>()
				.WithMessage("*ключевые зависимости*");

			Action unregister = () => orchestrator.UnregisterKey("productGroups", "anything");
			unregister.Should().Throw<InvalidOperationException>();

			// А для keyless стадии — работает.
			Action keylessRegister = () => orchestrator.RegisterKey("shops", "shop-1");
			keylessRegister.Should().NotThrow();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
