using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace JobOrchestrator.Hosting;

/// <summary>Регистрация SDK в DI-контейнере хоста.</summary>
public static class ServiceCollectionExtensions {
	/// <summary>Размер capacity для bounded-канала событий event loop'а. Backpressure-fail-fast при заполнении.</summary>
	private const int ChannelCapacity = 10_000;

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
		services.AddSingleton<InstanceManager>();
		services.AddSingleton<KeyspaceRegistry>();
		services.AddSingleton<InstanceCreator>();
		services.AddSingleton<StageRunner>();
		services.AddSingleton<DueScanner>();
		services.AddSingleton<EventLoop>();
		// Bounded channel: backpressure через Wait. При заполнении внешние писатели (Manual triggers,
		// RegisterKey) подождут места; event loop как consumer обычно их быстро разгружает.
		services.AddSingleton(_ => Channel.CreateBounded<OrchestratorEvent>(new BoundedChannelOptions(ChannelCapacity) {
			SingleReader = true,
			SingleWriter = false,
			FullMode = BoundedChannelFullMode.Wait,
		}));
		services.AddSingleton<OrchestratorLifecycle>();
		services.AddSingleton<IJobOrchestrator, JobOrchestratorRuntime>();
		services.AddHostedService<JobOrchestratorHostedService>();

		// TimeProvider может быть уже зарегистрирован хостом; иначе используем системное время.
		services.TryAddSingleton(TimeProvider.System);

		// Валидация startup: каждая стадия ссылается на ServiceType, реально зарегистрированный в DI.
		// Опускаем сюда snapshot уже-сформированной коллекции, чтобы не цеплять провайдер.
		ValidateServiceRegistrations(registry, services);

		return services;
	}

	/// <summary>
	/// Проверяет, что для каждой стадии <see cref="StageDescriptor.ServiceType"/> присутствует в DI-контейнере.
	/// SDK сам делает <see cref="ServiceCollectionDescriptorExtensions.TryAddScoped"/> для всех ServiceType,
	/// поэтому реально провалиться эта проверка может только при ручном <c>services.Remove(...)</c>
	/// между <c>AddJobOrchestrator</c> и хостом — но проще ловить такую ошибку громко.
	/// </summary>
	private static void ValidateServiceRegistrations(StageRegistry registry, IServiceCollection services) {
		var registered = new HashSet<Type>();
		foreach (var d in services) registered.Add(d.ServiceType);

		var missing = registry.AllStages
			.Where(s => !registered.Contains(s.ServiceType))
			.Select(s => $"{s.Name}({s.ServiceType.FullName})")
			.ToList();
		if (missing.Count > 0) {
			throw new JobConfigurationException(
				$"IJobService-реализации не зарегистрированы в DI: {string.Join(", ", missing)}.");
		}
	}
}
