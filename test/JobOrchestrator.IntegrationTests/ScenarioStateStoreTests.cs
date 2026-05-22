using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioStateStoreTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task IJobState_PersistsBetweenIterations() {
		// На первой итерации записываем в State, на следующей — читаем. Signal-based wait через TCS:
		// тест больше не зависит от точного интервала-таймера, ждёт ровно того, что нужно.
		var fake = new FakeServiceA();
		int call = 0;
		var readSignal = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
		fake.ExecuteHandler = async (ctx, _) => {
			int n = Interlocked.Increment(ref call);
			if (n == 1) {
				await ctx.State.SetAsync("cursor", "value-1").ConfigureAwait(false);
			} else if (n == 2) {
				var read = await ctx.State.GetAsync<string>("cursor").ConfigureAwait(false);
				readSignal.TrySetResult(read);
			}
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RunPeriodically(TimeSpan.FromMilliseconds(200)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			var readBack = await readSignal.Task.WaitAsync(Timeout).ConfigureAwait(false);
			readBack.Should().Be("value-1");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task UnregisterKey_RemovesInstanceScopeFromStateStore() {
		// pg[shops=u1] записывает в State; UnregisterKey(shops, u1) каскадно удаляет инстанс
		// и вызывает RemoveScopeAsync на IJobStateStore. Проверяем напрямую через store.
		var shopsFake = new FakeServiceA();
		var pgFake = new FakeServiceB();

		int shopsCall = 0;
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			if (shopsCall == 0) await ctx.AddKeyAsync("u1", ct);
			shopsCall++;
		};
		pgFake.ExecuteHandler = async (ctx, _) => {
			await ctx.State.SetAsync("data", "value-from-pg").ConfigureAwait(false);
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
			});

		var store = host.Services.GetRequiredService<IJobStateStore>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			var scopeName = "pg:shops=u1";
			// Дожидаемся, что pg.ExecuteAsync действительно сохранил данные (SetAsync завершился).
			(await TestSync.WaitForAsync(async () =>
				(await store.GetAsync(scopeName, "data", CancellationToken.None).ConfigureAwait(false)) is not null,
				Timeout
			).ConfigureAwait(false)).Should().BeTrue("pg должен был записать в State до UnregisterKey");

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			orchestrator.Root["shops"].UnregisterKey("u1");

			// Дожидаемся завершения каскада: scope полностью удалён через RemoveScopeAsync.
			(await TestSync.WaitForAsync(async () =>
				(await store.GetAsync(scopeName, "data", CancellationToken.None).ConfigureAwait(false)) is null,
				Timeout
			).ConfigureAwait(false)).Should().BeTrue("RemoveScopeAsync должен был очистить scope при каскадном удалении");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
