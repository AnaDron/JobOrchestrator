using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests.Support;

internal static class TestHostFactory {
	/// <summary>
	/// Создаёт <see cref="IHost"/> с зарегистрированным SDK и in-memory state store.
	/// Все <see cref="FakeJobServiceBase"/>-наследники, найденные через <paramref name="registerFakes"/>,
	/// регистрируются как singletons (общий экземпляр для всех scope-ов итераций).
	/// </summary>
	public static IHost Build(
		Action<JobOrchestratorBuilder> configure,
		Action<IServiceCollection>? registerFakes = null
	) {
		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();

		registerFakes?.Invoke(builder.Services);
		builder.Services.AddInMemoryJobStateStore();
		builder.Services.AddJobOrchestrator(configure);

		return builder.Build();
	}
}
