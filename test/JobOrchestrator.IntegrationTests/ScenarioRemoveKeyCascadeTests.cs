using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioRemoveKeyCascadeTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task RemoveKey_CascadesThroughTransitiveDependencies() {
		// shops → productGroups (DependsOnInstance) → products (DependsOn) → documents (DependsOn).
		// RemoveKey(shops, "u1") должен удалить pg[shops=u1], products[shops=u1], documents[shops=u1].
		// employees[] и инстансы для других uuid не должны быть тронуты.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				var pg = jobs.Stage("productGroups").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
				var products = jobs.Stage("products").HandledBy<FakeServiceC>().DependsOn(pg).RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("documents").HandledBy<FakeServiceD>().DependsOn(products).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
				s.AddSingleton<FakeServiceC>();
				s.AddSingleton<FakeServiceD>();
			});

		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = (ctx, _) => {
			ctx.AddKey("u1");
			ctx.AddKey("u2");
			return Task.CompletedTask;
		};

		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		var productsFake = host.Services.GetRequiredService<FakeServiceC>();
		var documentsFake = host.Services.GetRequiredService<FakeServiceD>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await documentsFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue("оба uuid каскадируются вниз");

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			orchestrator.UnregisterKey("shops", "u1");

			// Дать event loop'у обработать каскадное удаление.
			await Task.Delay(500).ConfigureAwait(false);

			var overview = await orchestrator.GetOverviewAsync().ConfigureAwait(false);
			var fqns = overview.Instances.Select(j => j.FullyQualifiedName).ToHashSet();

			// u1-ветвь удалена:
			fqns.Should().NotContain("productGroups[shops=u1]");
			fqns.Should().NotContain("products[shops=u1]");
			fqns.Should().NotContain("documents[shops=u1]");
			// u2-ветвь сохранилась:
			fqns.Should().Contain("productGroups[shops=u2]");
			fqns.Should().Contain("products[shops=u2]");
			fqns.Should().Contain("documents[shops=u2]");
			// shops[] (родитель) тоже сохранился:
			fqns.Should().Contain("shops[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
