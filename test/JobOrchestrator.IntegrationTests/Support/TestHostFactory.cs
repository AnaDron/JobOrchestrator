using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests.Support;

internal static class TestHostFactory {
	/// <summary>
	/// Создаёт <see cref="IHost"/> с зарегистрированным SDK и in-memory state store.
	/// Все <see cref="FakeJobServiceBase"/>-наследники, найденные через <paramref name="registerFakes"/>,
	/// регистрируются как singletons (общий экземпляр для всех scope-ов итераций).
	/// </summary>
	/// <param name="timeProvider">
	/// Если задан — регистрируется как singleton ДО <c>AddJobOrchestrator</c>, чтобы внутренний
	/// <c>TryAddSingleton(TimeProvider.System)</c> стал no-op. Используется для детерминированных
	/// тестов на основе <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>.
	/// </param>
	public static IHost Build(
		Action<JobOrchestratorBuilder> configure,
		Action<IServiceCollection>? registerFakes = null,
		TimeProvider? timeProvider = null
	) {
		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();

		if (timeProvider is not null) {
			builder.Services.AddSingleton(timeProvider);
		}
		builder.Services.AddJobOrchestrator(jobs => {
			jobs.UseInMemoryStateStore();
			configure(jobs);
		});
		// registerFakes — после остальных регистраций, чтобы тест мог переопределить любую (последняя
		// регистрация singleton выигрывает при resolve).
		registerFakes?.Invoke(builder.Services);

		return builder.Build();
	}
}
