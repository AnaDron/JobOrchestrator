using JobOrchestrator.Internal;
using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioCrashIsolationTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task AfterMarkFaulted_RunAsync_ThrowsFaulted() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var fake = host.Services.GetRequiredService<FakeServiceA>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			orchestrator.IsFaulted.Should().BeFalse();

			// Симулируем крах event loop через прямой MarkFaulted (internal API через InternalsVisibleTo).
			host.Services.GetRequiredService<JobOrchestratorRuntime>().MarkFaulted();
			orchestrator.IsFaulted.Should().BeTrue();

			Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync();
			var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
			ex.Reason.Should().Be(IterationRejectReason.Faulted);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task AfterMarkFaulted_RegisterUnregisterKey_ThrowsInvalidOperation() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			host.Services.GetRequiredService<JobOrchestratorRuntime>().MarkFaulted();

			Action register = () => orchestrator.Root["a"].RegisterKey("k1");
			register.Should().Throw<InvalidOperationException>();

			Action unregister = () => orchestrator.Root["a"].UnregisterKey("k1");
			unregister.Should().Throw<InvalidOperationException>();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task AfterMarkFaulted_GetOverview_IsStillAccessible() {
		// Новый контракт (см. ultrathink-анализ, P3.5.2): GetOverview доступен даже в Faulted-состоянии,
		// чтобы можно было увидеть post-mortem snapshot — что было в InstanceManager на момент краша.
		// Мутирующие операции (RunAsync/RegisterKey) — fail-fast как раньше.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			host.Services.GetRequiredService<JobOrchestratorRuntime>().MarkFaulted();

			// GetOverview не throws — даёт snapshot для диагностики.
			var act = () => orchestrator.GetOverview();
			act.Should().NotThrow();
			orchestrator.IsFaulted.Should().BeTrue();

			// Но мутирующие операции — throws.
			Action register = () => orchestrator.Root["a"].RegisterKey("k1");
			register.Should().Throw<InvalidOperationException>();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task IJobServiceThrows_ItIsCapturedAsStageFailed_EventLoopRemainsAlive() {
		// IJobService с исключением → StageRunner ловит и публикует StageFailedEvent.
		// Event loop обрабатывает как обычный неуспех, обновляет ConsecutiveFailures.
		// Это per-iteration изоляция — НЕ event loop crash, но проверяет нормальный путь восстановления.
		var fake = new FakeServiceA();
		int call = 0;
		fake.ExecuteHandler = (ctx, _) => {
			call++;
			throw new InvalidOperationException($"call {call} failed");
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromMilliseconds(50)))
				.RunPeriodically(TimeSpan.FromMilliseconds(100)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();

			orchestrator.IsFaulted.Should().BeFalse("per-iteration исключения не валят event loop");
			var overview = orchestrator.GetOverview();
			var info = overview.Instances.Single();
			info.ConsecutiveFailures.Should().BeGreaterThan(0);
			info.LastError.Should().Contain("failed");
			info.LastSuccess.Should().BeNull();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task StateStoreThrowsOnRemoveScope_KeyRemoveCascadeStillCompletes() {
		// IJobStateStore.RemoveScopeAsync падает; внутренний catch в HandleKeyRemovedAsync
		// ловит и логирует Warning. Event loop продолжает обрабатывать события.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
				// Заменяем дефолтный InMemoryJobStateStore на броcаемый — register перед AddJobOrchestrator.
				s.AddSingleton<IJobStateStore, ThrowingJobStateStore>();
			});

		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };

		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();

			orchestrator.Root["shops"].UnregisterKey("u1");
			await Task.Delay(300).ConfigureAwait(false);

			// Event loop жив, орchestratorфункционален несмотря на исключение store-а.
			orchestrator.IsFaulted.Should().BeFalse();
			var overview = orchestrator.GetOverview();
			overview.Instances.Select(i => i.FullyQualifiedName).Should().NotContain("pg[shops=u1]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	private sealed class ThrowingJobStateStore : IJobStateStore {
		public Task<string?> GetAsync(string scope, string key, CancellationToken ct) => Task.FromResult<string?>(null);
		public Task SetAsync(string scope, string key, string value, CancellationToken ct) => Task.CompletedTask;
		public Task RemoveAsync(string scope, string key, CancellationToken ct) => Task.CompletedTask;
		public Task RemoveScopeAsync(string scope, CancellationToken ct) =>
			Task.FromException(new InvalidOperationException("simulated store failure"));
	}
}
