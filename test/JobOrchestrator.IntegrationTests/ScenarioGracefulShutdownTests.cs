using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioGracefulShutdownTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task RunAsync_AfterStop_ThrowsFaulted() {
		// После остановки SDK external-вызовы должны быстро бросать IterationRejectedException(Faulted),
		// не подвисать на await.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var fake = host.Services.GetRequiredService<FakeServiceA>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
		await host.StopAsync().ConfigureAwait(false);

		using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		Func<Task> act = () => orchestrator.Root["a"][InstanceKeys.Empty].RunAsync(cts.Token);
		var ex = (await act.Should().ThrowAsync<IterationRejectedException>().ConfigureAwait(false)).Which;
		ex.Reason.Should().Be(IterationRejectReason.Faulted);
	}

	[Fact]
	public async Task RegisterKey_AfterStop_ThrowsInvalidOperationOrSilent() {
		// RegisterKey после shutdown: возможны три исхода (зависит от тайминга bootstrap vs stop):
		// - keyless-инстанс остался в InstanceManager → silent (TryWrite на closed channel → false).
		// - keyless-инстанс ещё не создан bootstrap-ом до StopAsync → InvalidOperationException.
		// - событие отправлено, но дальше игнорируется.
		// Все три — корректный graceful-shutdown-behavior; контракт: НЕ ronqueve unexpected exception.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		await host.StartAsync().ConfigureAwait(false);
		await host.StopAsync().ConfigureAwait(false);

		Action act = () => orchestrator.Root["a"].RegisterKey("key1");
		// Либо silent (NotThrow), либо InvalidOperationException — оба ОК.
		try { act(); } catch (InvalidOperationException) { /* допустимо */ }
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
