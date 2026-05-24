namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// xUnit class-fixture: один <see cref="IHost"/> с keyless-стадией "a" на все тесты класса,
/// которые только читают handle-API (без RunAsync/RegisterKey/AddKey).
/// <para>
/// Read-only-семантика гарантирует, что shared-state не приведёт к cross-test conflict'у.
/// Серийное исполнение тестов класса обеспечивается <c>parallelizeTestCollections=false</c>
/// в <c>xunit.runner.json</c>.
/// </para>
/// </summary>
public sealed class KeylessAHostFixture : IAsyncLifetime {
	public IHost Host { get; } = HandleApiTestHelpers.BuildKeylessAHost();
	public IJobOrchestrator Orchestrator => Host.Services.GetRequiredService<IJobOrchestrator>();

	public async Task InitializeAsync() {
		await Host.StartAsync().ConfigureAwait(false);
	}

	public async Task DisposeAsync() {
		await Host.StopAsync().ConfigureAwait(false);
		Host.Dispose();
	}
}
