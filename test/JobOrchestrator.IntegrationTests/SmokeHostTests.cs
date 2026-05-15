namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Минимальный smoke-test: host строится, оркестратор резолвится из DI, GetOverview работает.
/// Быстрая CI-проверка sanity всей DI-pipeline и hosted-service lifecycle.
/// </summary>
public sealed class SmokeHostTests {
	[Fact]
	public async Task Host_Starts_And_Orchestrator_Resolves() {
		var builder = Host.CreateApplicationBuilder();
		builder.Services.AddLogging();
		builder.Services.AddScoped<NoopJob>();
		builder.Services.AddInMemoryJobStateStore();
		builder.Services.AddJobOrchestrator(jobs => {
			jobs.Stage("noop").HandledBy<NoopJob>().RunPeriodically(TimeSpan.FromHours(1));
		});

		using var host = builder.Build();
		await host.StartAsync().ConfigureAwait(false);
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		orchestrator.GetOverview().Instances.Should().NotBeNull();
		orchestrator.IsFaulted.Should().BeFalse();
		await host.StopAsync().ConfigureAwait(false);
	}

	private sealed class NoopJob : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}
}
