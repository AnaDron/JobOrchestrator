using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Контракт: стадия с двумя <c>DependsOnInstance(X)</c> + <c>DependsOnInstance(Y)</c> производит
/// cartesian-product инстансов — по одному на каждую пару (x_key, y_key). Это базовая семантика
/// multi-dimensional fan-out.
/// </summary>
public sealed class ScenarioMultiInstanceCartesianTests {
	[Fact]
	public async Task TwoDependsOnInstance_ProduceCartesianProduct() {
		var recorder = new ExecutionRecorder();
		var shopsKeys = new ShopsKeySource();
		shopsKeys.EnqueueAdds("s-A", "s-B");
		var curKeys = new CurrenciesKeySource();
		curKeys.Enqueue("USD", "RUB");

		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Warning);
		b.Services.AddSingleton(recorder);
		b.Services.AddSingleton(shopsKeys);
		b.Services.AddSingleton(curKeys);
		b.Services.AddScoped<ShopsStageService>();
		b.Services.AddScoped<CurrenciesStageService>();
		b.Services.AddScoped<DocumentsStageService>();
		b.Services.AddJobOrchestrator(jobs => {
			jobs.UseInMemoryStateStore();
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromMilliseconds(50));
			var shops = jobs.Stage("shops")
				.HandledBy<ShopsStageService>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			var currencies = jobs.Stage("currencies")
				.HandledBy<CurrenciesStageService>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			jobs.Stage("documents")
				.HandledBy<DocumentsStageService>()
				.DependsOnInstance(shops)
				.DependsOnInstance(currencies)
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
		});

		using var host = b.Build();
		await host.StartAsync().ConfigureAwait(false);

		try {
			await AsyncWait.UntilAsync(() => recorder.Count("documents") >= 4, TimeSpan.FromSeconds(5),
				"должны появиться 4 documents — cartesian product 2×2 (shops × currencies)");

			var pairs = recorder.ForStage("documents")
				.Select(e => (e.Keys["shops"], e.Keys["currencies"]))
				.ToHashSet();

			pairs.Should().BeEquivalentTo([
				("s-A", "USD"), ("s-A", "RUB"),
				("s-B", "USD"), ("s-B", "RUB"),
			]);
		} finally {
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}
}
