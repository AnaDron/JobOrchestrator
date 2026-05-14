using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// 3.1: E2E-тест на multi-instance-эмитер + per-emitter keyspace + cascade-removal.
/// <para>
/// Сценарий из ultrathink-анализа:
/// </para>
/// <list type="bullet">
/// <item><c>regions[]</c> — keyless, эмитит "EU" и "US";</item>
/// <item><c>shops</c> с <c>DependsOnInstance(regions)</c> → <c>shops[regions=EU]</c> и <c>shops[regions=US]</c> — каждый эмитит свои uuid;</item>
/// <item><c>products</c> с <c>DependsOnInstance(shops)</c> → инстансы на каждую пару (regions, shops);</item>
/// <item>UnregisterKey(regions, "EU") должен каскадно удалить ТОЛЬКО ветку regions=EU, не затронув US.</item>
/// </list>
/// <para>
/// Главная защёлка от регрессии Phase 1.2: ранее keyspace был плоский (Dictionary&lt;StageName, HashSet&gt;),
/// и при удалении одного multi-instance-эмитера нельзя было различить его ключи от ключей другого инстанса
/// той же стадии. Per-emitter buckets с RemoveInstance закрывает эту дыру.
/// </para>
/// </summary>
public sealed class ScenarioMultiInstanceEmitterCascadeTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task UnregisterKey_OnMultiInstanceEmitter_CascadesOnlyAffectedBranch() {
		var regionsFake = new FakeServiceA();
		regionsFake.ExecuteHandler = (ctx, _) => {
			ctx.AddKey("EU");
			ctx.AddKey("US");
			return Task.CompletedTask;
		};

		var shopsFake = new FakeServiceB();
		shopsFake.ExecuteHandler = (ctx, _) => {
			// shops[regions=EU] эмитит eu-1, shops[regions=US] эмитит us-1.
			var region = ctx.DependencyKeys["regions"];
			ctx.AddKey($"{region.ToLowerInvariant()}-1");
			return Task.CompletedTask;
		};

		var productsFake = new FakeServiceC();

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var regions = jobs.Stage("regions")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(regions)
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("products")
					.HandledBy<FakeServiceC>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(regionsFake);
				s.AddSingleton<FakeServiceB>(shopsFake);
				s.AddSingleton<FakeServiceC>(productsFake);
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			// Ждём пока products получит 2 итерации (по одной на каждую пару (region, shop)).
			(await productsFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var beforeRemove = orchestrator.GetOverview();
			beforeRemove.Instances.Should().HaveCount(5,
				"regions[] + shops[regions=EU] + shops[regions=US] + products[regions=EU,shops=eu-1] + products[regions=US,shops=us-1]");

			// Удаляем регион EU. По цепочке: shops[regions=EU] cascade'нется, и от него же
			// удалится bucket с ключом "eu-1" → products[regions=EU,shops=eu-1] тоже cascade'нется.
			orchestrator["regions"].UnregisterKey("EU");

			// Дать event loop'у обработать каскад.
			var settled = await TestSync.WaitForAsync(() => {
				var snap = orchestrator.GetOverview();
				return snap.Instances.Count == 3;    // regions + shops[US] + products[US,us-1]
			}, TimeSpan.FromSeconds(5)).ConfigureAwait(false);
			settled.Should().BeTrue("каскад должен оставить только не-EU-ветку");

			var after = orchestrator.GetOverview();
			var fqns = after.Instances.Select(i => i.FullyQualifiedName).OrderBy(x => x).ToList();
			fqns.Should().Contain("regions[]");
			fqns.Should().Contain("shops[regions=US]");
			fqns.Should().Contain("products[regions=US,shops=us-1]");
			fqns.Should().NotContain(x => x.Contains("regions=EU"),
				"US-ветка не затронута; EU-ветка целиком ушла");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
