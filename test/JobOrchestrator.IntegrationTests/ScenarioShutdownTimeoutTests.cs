using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// <see cref="JobOrchestratorHostOptions.ShutdownIterationTimeout"/> форсирует cancel running-итераций
/// после <see cref="Internal.JobOrchestratorRuntime.CloseChannel"/>.
/// </summary>
public sealed class ScenarioShutdownTimeoutTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task StopAsync_WithShutdownIterationTimeout_CancelsLongRunningIteration() {
		var iterationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var fake = new FakeServiceA();
		fake.ExecuteHandler = async (_, ct) => {
			iterationStarted.TrySetResult();
			await Task.Delay(TimeSpan.FromDays(1), ct);
		};

		using var host = TestHostFactory.Build(
			configure: jobs => {
				jobs.ConfigureJobOrchestratorHost(o => o.ShutdownIterationTimeout = TimeSpan.FromMilliseconds(300));
				jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => s.AddSingleton(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			await iterationStarted.Task.WaitAsync(Timeout).ConfigureAwait(false);

			var stopTask = host.StopAsync();
			await stopTask.WaitAsync(Timeout).ConfigureAwait(false);

			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// После StopAsync итерация должна была получить cancel (не висеть бесконечно).
			await Task.Delay(500).ConfigureAwait(false);
			host.Services.GetRequiredService<IJobOrchestrator>().IsFaulted.Should().BeFalse();
		} finally {
			if (host is IAsyncDisposable ad) await ad.DisposeAsync().ConfigureAwait(false);
		}
	}
}
