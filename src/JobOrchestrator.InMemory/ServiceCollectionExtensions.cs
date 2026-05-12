using JobOrchestrator.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.InMemory;

/// <summary>Регистрация in-memory backend для <see cref="IJobStateStore"/>.</summary>
public static class ServiceCollectionExtensions {
	/// <summary>Регистрирует <see cref="InMemoryJobStateStore"/> как singleton реализацию <see cref="IJobStateStore"/>.</summary>
	public static IServiceCollection AddInMemoryJobStateStore(this IServiceCollection services) {
		ArgumentNullException.ThrowIfNull(services);
		services.AddSingleton<IJobStateStore, InMemoryJobStateStore>();
		return services;
	}
}
