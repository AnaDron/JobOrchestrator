using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Integration-тест ConcurrencyLimit: с multi-instance-стадией, у которой лимит=2, никогда более 2-х
/// итераций не выполняется одновременно. Защёлка от регрессии — без семафора в EventLoop все инстансы
/// запустились бы параллельно.
/// </summary>
public sealed class ScenarioConcurrencyLimitTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task StageWithConcurrencyLimit_NeverExceedsParallelLimit() {
		const int Limit = 2;
		const int TotalKeys = 6;

		int currentlyRunning = 0;
		int peakObserved = 0;
		var gate = new object();

		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = (ctx, _) => {
			for (int i = 0; i < TotalKeys; i++) ctx.AddKey($"shop-{i}");
			return Task.CompletedTask;
		};

		var pgFake = new FakeServiceB();
		pgFake.ExecuteHandler = async (ctx, ct) => {
			int running;
			lock (gate) {
				running = ++currentlyRunning;
				if (running > peakObserved) peakObserved = running;
			}
			try {
				await Task.Delay(150, ct).ConfigureAwait(false);
			} finally {
				lock (gate) { currentlyRunning--; }
			}
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.WithConcurrencyLimit(Limit)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(TotalKeys, Timeout).ConfigureAwait(false)).Should().BeTrue();
			peakObserved.Should().BeLessThanOrEqualTo(Limit,
				$"стадия с ConcurrencyLimit={Limit} никогда не должна запускать больше параллельно (наблюдалось peak={peakObserved})");
			peakObserved.Should().BeGreaterThan(0, "хотя бы одна итерация должна выполниться");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
