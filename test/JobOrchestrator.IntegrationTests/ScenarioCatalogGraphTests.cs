using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Сценарии на полной canonical-топологии (shops → productGroups → products → documents + employees),
/// собранной через <see cref="TestHostBuilder.BuildCatalogGraph"/>. Демонстрирует use-case helper-а и
/// проверяет key-propagation через многоуровневую цепочку <c>DependsOnInstance</c> + <c>DependsOn</c>.
/// </summary>
public sealed class ScenarioCatalogGraphTests {
	[Fact]
	public async Task ThreeShops_Propagate_DownThroughGraph_To_Documents() {
		var recorder = new ExecutionRecorder();
		var shops = new ShopsKeySource();
		shops.EnqueueAdds("s-1", "s-2", "s-3");

		using var host = TestHostBuilder.BuildCatalogGraph(recorder, shops);
		await host.StartAsync().ConfigureAwait(false);
		try {
			await AsyncWait.UntilAsync(() => recorder.Count("documents") >= 3, TimeSpan.FromSeconds(5),
				"documents должны появиться по одному на каждый shops-ключ");

			var docsShops = recorder.ForStage("documents")
				.Select(e => e.Keys["shops"])
				.ToHashSet(StringComparer.Ordinal);
			docsShops.Should().BeEquivalentTo(["s-1", "s-2", "s-3"]);

			var productsShops = recorder.ForStage("products")
				.Select(e => e.Keys["shops"])
				.ToHashSet(StringComparer.Ordinal);
			productsShops.Should().BeEquivalentTo(["s-1", "s-2", "s-3"]);

			// employees — keyless (DependsOn(shops) с пустыми ключами).
			recorder.ForStage("employees").Should().NotBeEmpty();
			recorder.ForStage("employees").Select(e => e.Keys.Count).Should().AllSatisfy(c => c.Should().Be(0));
		} finally {
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}
}
