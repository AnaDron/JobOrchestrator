using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioMonotonicLastSuccessTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task ParentSucceedsOnceThenFails_DependentStageContinuesToRun() {
		// Инвариант монотонности LastSuccess: после первого успеха родителя зависимая стадия
		// должна продолжать создаваться и работать, даже если родитель уходит в череду падений.
		var shopsFake = new FakeServiceA();
		var pgFake = new FakeServiceB();

		int shopsCall = 0;
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			shopsCall++;
			if (shopsCall == 1) {
				await ctx.AddKeyAsync("u1", ct);
				return;
			}
			throw new InvalidOperationException($"shops call {shopsCall} fails");
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromMilliseconds(50)))
					.RunPeriodically(TimeSpan.FromMilliseconds(100));
				jobs.Stage("pg")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromMilliseconds(100));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			// Ждём пока shops упадёт хотя бы 3 раза (после первого успеха).
			while (shopsCall < 4) {
				await Task.Delay(50).ConfigureAwait(false);
				if (shopsCall >= 4) break;
			}

			// pg должен работать несмотря на постоянные падения shops после первого успеха.
			(await pgFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue(
				"pg создан после первого успеха shops; LastSuccess монотонна и зависимость остаётся разрешённой");

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var overview = orchestrator.GetOverview();
			var shopsInfo = overview.Instances.Single(i => i.StageName == "shops");
			shopsInfo.LastSuccess.Should().NotBeNull("первый успех был — LastSuccess монотонна");
			shopsInfo.ConsecutiveFailures.Should().BeGreaterThan(0, "сейчас в серии падений");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
