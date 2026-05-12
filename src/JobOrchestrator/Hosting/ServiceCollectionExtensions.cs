using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace JobOrchestrator.Hosting;

/// <summary>Регистрация SDK в DI-контейнере хоста.</summary>
public static class ServiceCollectionExtensions {
	/// <summary>
	/// Регистрирует Job Orchestrator с графом стадий, описанным в <paramref name="configure"/>.
	/// Также регистрирует все <see cref="IJobService"/>-реализации как scoped в DI (per-итерация scope).
	/// </summary>
	/// <remarks>
	/// Требует, чтобы в контейнере был зарегистрирован <see cref="IJobStateStore"/> — например, через
	/// <c>services.AddInMemoryJobStateStore()</c> (пакет <c>JobOrchestrator.InMemory</c>) или внешний backend.
	/// </remarks>
	public static IServiceCollection AddJobOrchestrator(
		this IServiceCollection services,
		Action<JobOrchestratorBuilder> configure
	) {
		ArgumentNullException.ThrowIfNull(services);
		ArgumentNullException.ThrowIfNull(configure);

		var builder = new JobOrchestratorBuilder();
		configure(builder);
		var registry = builder.BuildRegistry();

		foreach (var stage in registry.AllStages) {
			services.TryAddScoped(stage.ServiceType);
		}

		services.AddSingleton(registry);
		services.AddSingleton<JobManager>();
		services.AddSingleton<KeyspaceRegistry>();
		services.AddSingleton<InstanceCreator>();
		services.AddSingleton<StageRunner>();
		services.AddSingleton<EventLoop>();
		services.AddSingleton(_ => Channel.CreateUnbounded<OrchestratorEvent>(
			new UnboundedChannelOptions { SingleReader = true, SingleWriter = false }));
		services.AddSingleton<IJobOrchestrator, JobOrchestratorRuntime>();
		services.AddHostedService<JobOrchestratorHostedService>();

		// TimeProvider может быть уже зарегистрирован хостом; иначе используем системное время.
		services.TryAddSingleton(TimeProvider.System);

		return services;
	}
}
