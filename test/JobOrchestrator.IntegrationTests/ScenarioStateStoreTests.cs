using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioStateStoreTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task IJobState_PersistsBetweenIterations() {
		// На первой итерации записываем в State, на следующей — читаем.
		var fake = new FakeServiceA();
		int call = 0;
		string? readBack = null;
		fake.ExecuteHandler = async (ctx, _) => {
			call++;
			if (call == 1) {
				await ctx.State.SetAsync("cursor", "value-1").ConfigureAwait(false);
			} else if (call == 2) {
				readBack = await ctx.State.GetAsync<string>("cursor").ConfigureAwait(false);
			}
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RunPeriodically(TimeSpan.FromMilliseconds(100)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
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
		shopsFake.ExecuteHandler = (ctx, _) => {
			if (shopsCall == 0) ctx.AddKey("u1");
			shopsCall++;
			return Task.CompletedTask;
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
			await Task.Delay(200).ConfigureAwait(false);

			// До UnregisterKey: scope pg[shops=u1] существует и содержит данные.
			var scopeName = "pg:shops=u1";
			(await store.GetAsync(scopeName, "data", CancellationToken.None).ConfigureAwait(false))
				.Should().NotBeNull();

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			orchestrator.UnregisterKey("shops", "u1");
			await Task.Delay(300).ConfigureAwait(false);

			// После UnregisterKey: scope полностью удалён через RemoveScopeAsync.
			(await store.GetAsync(scopeName, "data", CancellationToken.None).ConfigureAwait(false))
				.Should().BeNull("RemoveScopeAsync должен был очистить scope при каскадном удалении");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
