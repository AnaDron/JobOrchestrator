using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioManualTriggerTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task TriggerAsync_UnknownInstance_ReturnsNotFound() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var result = await orchestrator.TriggerAsync("nonexistent").ConfigureAwait(false);
			result.Should().Be(TriggerResult.NotFound);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task TriggerAsync_AfterFailureInRetryDelay_ManualIgnoresIt() {
		// Конфиг: длинный retry-delay; вызываем итерацию, она падает; сразу же делаем Manual trigger.
		// Auto был бы WaitingRetry, Manual должен принять.
		var fake = new FakeServiceA();
		int calls = 0;
		fake.ExecuteHandler = (ctx, _) => {
			calls++;
			if (calls == 1) throw new InvalidOperationException("first call fails");
			return Task.CompletedTask;
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromMinutes(10)))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// Дать event loop'у обработать StageFailed.
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var result = await orchestrator.TriggerAsync("a").ConfigureAwait(false);
			result.Should().Be(TriggerResult.Started, "Manual игнорирует retry-delay");

			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task TriggerAsync_InDebounceWindow_ReturnsDebounced() {
		var fake = new FakeServiceA();
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a")
				.HandledBy<FakeServiceA>()
				.Debounce(TimeSpan.FromMinutes(10))
				.RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>(fake));

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// Дать event loop'у обработать StageCompleted — выставить LastAttempt.
			await Task.Delay(200).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var result = await orchestrator.TriggerAsync("a").ConfigureAwait(false);
			result.Should().Be(TriggerResult.Debounced);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
