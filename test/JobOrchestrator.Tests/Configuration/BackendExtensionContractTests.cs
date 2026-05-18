using JobOrchestrator.Hosting;
using JobOrchestrator.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Tests.Configuration;

/// <summary>
/// Контракт backend-extension'ов на <see cref="JobOrchestratorBuilder"/>:
/// <list type="number">
/// <item>На probe-pass (builder построен через <c>new JobOrchestratorBuilder(services, tenantKey?)</c>)
/// extension регистрирует backend в переданном <see cref="IServiceCollection"/>.</item>
/// <item>На lazy-pass (default-ctor builder, переиспользуемый DI как singleton) <see cref="JobOrchestratorBuilder.Services"/>
/// равен <c>null</c>, и extension должен no-op'нуться через явный guard.</item>
/// </list>
/// Эти тесты страхуют поведение SDK-extension'а <c>UseInMemoryStateStore</c> и одновременно служат
/// reference-документацией для авторов внешних backend-пакетов (Redis/Postgres/etc).
/// </summary>
public sealed class BackendExtensionContractTests {
	[Fact]
	public void UseInMemoryStateStore_OnProbePassBuilder_RegistersNonKeyedSingleton() {
		var services = new ServiceCollection();
		var builder = new JobOrchestratorBuilder(services, tenantKey: null);

		builder.UseInMemoryStateStore();

		services.Should().ContainSingle(d =>
			d.ServiceType == typeof(IJobStateStore) && !d.IsKeyedService);
	}

	[Fact]
	public void UseInMemoryStateStore_OnProbePassBuilderWithTenant_RegistersKeyedSingleton() {
		var services = new ServiceCollection();
		var builder = new JobOrchestratorBuilder(services, tenantKey: "evotor");

		builder.UseInMemoryStateStore();

		services.Should().ContainSingle(d =>
			d.ServiceType == typeof(IJobStateStore)
			&& d.IsKeyedService
			&& Equals(d.ServiceKey, "evotor"));
	}

	[Fact]
	public void UseInMemoryStateStore_OnLazyPassBuilder_IsNoOp() {
		// Default-ctor имитирует lazy-pass: Services=null, TenantKey=null. Корректный extension
		// должен распознать это и тихо выйти, не бросая NRE.
		var builder = new JobOrchestratorBuilder();
		builder.Services.Should().BeNull();

		Action act = () => builder.UseInMemoryStateStore();

		act.Should().NotThrow("на lazy-pass backend-extension обязан no-op'нуться через `Services is null` guard");
	}

	[Fact]
	public void AddJobOrchestrator_WithoutAnyStateStore_ThrowsAtFirstStageRegistryResolve() {
		// Пользователь забыл вызвать jobs.UseInMemoryStateStore() — SDK ловит это при первом resolve
		// StageRegistry с понятным сообщением, а не позволяет работать с молча отсутствующим store.
		var services = new ServiceCollection();
		services.AddLogging();
		services.AddJobOrchestrator(jobs =>
			jobs.Stage("solo").HandledBy<FakeJob>().RunPeriodically(TimeSpan.FromMinutes(1)));

		using var sp = services.BuildServiceProvider();
		Action act = () => sp.GetRequiredService<StageRegistry>();

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*IJobStateStore*UseInMemoryStateStore*");
	}

	[Fact]
	public void AddJobOrchestrator_KeyedWithoutStateStore_ThrowsWithTenantInMessage() {
		// Tenant'овая регистрация без UseInMemoryStateStore — должна бросать с упоминанием tenantKey,
		// чтобы пользователь сразу понял в каком конкретно AddJobOrchestrator(...)-блоке забыт store.
		var services = new ServiceCollection();
		services.AddLogging();
		services.AddJobOrchestrator("evotor", jobs =>
			jobs.WithDomain("evotor", e =>
				e.Stage("shops").HandledBy<FakeJob>().RunPeriodically(TimeSpan.FromMinutes(1))));

		using var sp = services.BuildServiceProvider();
		Action act = () => sp.GetRequiredKeyedService<StageRegistry>("evotor");

		act.Should().Throw<InvalidOperationException>()
			.WithMessage("*\"evotor\"*UseInMemoryStateStore*");
	}

	private sealed class FakeJob : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}
}
