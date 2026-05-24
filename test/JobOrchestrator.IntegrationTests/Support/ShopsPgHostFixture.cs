namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// xUnit class-fixture: один <see cref="IHost"/> с графом shops + pg(DependsOnInstance(shops))
/// на все тесты класса, которые только читают handle-API. См. <see cref="KeylessAHostFixture"/>
/// для обоснования shared-host подхода.
/// </summary>
public sealed class ShopsPgHostFixture : IAsyncLifetime {
	public IHost Host { get; } = HandleApiTestHelpers.BuildShopsPgHost();
	public IJobOrchestrator Orchestrator => Host.Services.GetRequiredService<IJobOrchestrator>();

	public async Task InitializeAsync() {
		await Host.StartAsync().ConfigureAwait(false);
	}

	public async Task DisposeAsync() {
		await Host.StopAsync().ConfigureAwait(false);
		Host.Dispose();
	}
}
