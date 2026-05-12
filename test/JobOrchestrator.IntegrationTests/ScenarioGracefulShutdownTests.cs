using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioGracefulShutdownTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task TriggerAsync_AfterStop_ReturnsFaultedOrNotFound() {
		// После остановки SDK external-вызовы должны быстро возвращать Faulted, не подвисать на await.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var fake = host.Services.GetRequiredService<FakeServiceA>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
		await host.StopAsync().ConfigureAwait(false);

		// После shutdown TriggerAsync должен либо вернуться Faulted (Channel closed), либо завершиться быстро (<1сек).
		// Используем CancellationToken с таймаутом для защиты от подвисания.
		using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var result = await orchestrator.TriggerAsync("a", null, cts.Token).ConfigureAwait(false);
		result.Should().Be(TriggerResult.Faulted);
	}

	[Fact]
	public async Task RegisterKey_AfterStop_ThrowsInvalidOperationOrSilent() {
		// RegisterKey после shutdown: либо InvalidOperationException (если IsFaulted), либо silent TryWrite false.
		// Текущая semantics: graceful shutdown НЕ выставляет IsFaulted, но Channel закрыт.
		// TryWrite на closed channel вернёт false — silently игнорируется.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		await host.StopAsync().ConfigureAwait(false);

		// RegisterKey не упадёт с exception, но event не будет обработан — это OK для graceful shutdown.
		Action act = () => orchestrator.RegisterKey("a", "key1");
		act.Should().NotThrow();
	}

	[Fact]
	public async Task RunningIteration_OnStop_GracefullyCancelled() {
		// Долгая running-итерация должна получить cancel через stoppingToken при host.StopAsync.
		var fake = new FakeServiceA();
		var startSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var cancelObserved = false;
		fake.ExecuteHandler = async (ctx, ct) => {
			startSignal.TrySetResult();
			try {
				await Task.Delay(TimeSpan.FromMinutes(1), ct).ConfigureAwait(false);
			} catch (OperationCanceledException) {
				cancelObserved = true;
				throw;
			}
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		await startSignal.Task.WaitAsync(Timeout).ConfigureAwait(false);  // дождаться начала итерации

		await host.StopAsync().ConfigureAwait(false);
		cancelObserved.Should().BeTrue("долгая итерация должна получить cancel через stoppingToken на shutdown");
	}
}
