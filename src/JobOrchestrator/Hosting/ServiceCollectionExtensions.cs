using System.Threading.Channels;
using JobOrchestrator.Hosting.Keyed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Hosting;

/// <summary>Регистрация SDK в DI-контейнере хоста.</summary>
public static class ServiceCollectionExtensions {
	/// <summary>Размер capacity для bounded-канала событий event loop'а. Backpressure-fail-fast при заполнении.</summary>
	private const int ChannelCapacity = 10_000;

	/// <summary>
	/// Регистрирует Job Orchestrator с графом стадий, описанным в <paramref name="configure"/>.
	/// Также регистрирует все <see cref="IJobService"/>-реализации как scoped в DI (per-итерация scope).
	/// <para>
	/// <b>Multi-call accumulation</b>: метод можно вызывать многократно — каждая независимая
	/// integration-библиотека предоставляет свой extension-метод (<c>services.AddEvotorJobs()</c>,
	/// <c>services.AddOzonJobs()</c>), внутри дёргающий <c>AddJobOrchestrator</c>. Все <paramref name="configure"/>-actions
	/// аккумулируются и применяются к одному shared <see cref="JobOrchestratorBuilder"/> при первом
	/// resolve'е <see cref="Internal.StageRegistry"/>.
	/// </para>
	/// <para>
	/// <b>Контракт идемпотентности</b>: <paramref name="configure"/>-action может быть выполнен дважды
	/// (probe-pass для eager-валидации + lazy-pass при resolve). Action не должен иметь side-effects
	/// вне переданного <see cref="JobOrchestratorBuilder"/>.
	/// </para>
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

		// Probe-pass: configure выполняется на throwaway-builder'е для:
		// (a) eager per-call валидации через BuildRegistry (HandledBy, RunPeriodically, циклы внутри
		//     этого configure, дубль имён внутри этого configure),
		// (b) извлечения StageServiceType-ов для TryAddScoped — это нужно сделать СЕЙЧАС, поскольку
		//     DI-registrations нельзя добавить после BuildServiceProvider.
		// Cross-call валидация (collision имён между двумя AddJobOrchestrator-вызовами) откладывается
		// до первого resolve StageRegistry.
		// Probe-builder получает прямую ссылку на services + tenantKey=null. Backend-extension'ы
		// внутри configure (например, jobs.UseInMemoryStateStore()) сразу регистрируют свои сервисы
		// на основе builder.Services / builder.TenantKey.
		var probe = new JobOrchestratorBuilder(services, tenantKey: null);
		configure(probe);
		var probeRegistry = probe.BuildRegistry();
		foreach (var stage in probeRegistry.AllStages) {
			services.TryAddScoped(stage.ServiceType);
		}

		// Регистрация infrastructure — однократно, на ПЕРВОМ вызове. Используем StageRegistry-маркер
		// как индикатор «инфра уже зарегистрирована».
		EnsureInfrastructureRegistered(services);

		// Append configure в accumulator — multi-singleton-registration.
		services.AddSingleton<IJobsConfigure>(new JobsConfigure(configure));

		return services;
	}

	/// <summary>
	/// Регистрирует Job Orchestrator для конкретного <paramref name="tenantKey"/> с keyed-services
	/// изоляцией (Path A). Каждый tenant получает свой полный набор keyed-singleton'ов:
	/// <see cref="Internal.StageRegistry"/>, <see cref="Internal.InstanceManager"/>, channel, event loop,
	/// lifecycle, etc. <see cref="Microsoft.Extensions.Hosting.IHostedService"/> регистрируется по одному
	/// на tenant — .NET host стартует все.
	/// <para>
	/// Stage-services регистрируются как <c>AddKeyedScoped(stage.ServiceType, tenantKey)</c>, что
	/// позволяет двум tenant'ам использовать один и тот же тип <see cref="IJobService"/> с независимыми DI-scope.
	/// </para>
	/// <para>
	/// <b>Multi-call accumulation per-tenant</b>: метод можно вызывать многократно с одним
	/// <paramref name="tenantKey"/> — accumulator-pattern работает per-tenant.
	/// </para>
	/// </summary>
	public static IServiceCollection AddJobOrchestrator(
		this IServiceCollection services,
		string tenantKey,
		Action<JobOrchestratorBuilder> configure
	) {
		ArgumentNullException.ThrowIfNull(services);
		ArgumentException.ThrowIfNullOrEmpty(tenantKey);
		ArgumentNullException.ThrowIfNull(configure);

		// Probe-builder с прямым доступом к services и текущим tenantKey. Backend-extension'ы
		// (jobs.UseInMemoryStateStore() и т.п.) сразу регистрируют keyed-backend под этим ключом.
		var probe = new JobOrchestratorBuilder(services, tenantKey);
		configure(probe);
		var probeRegistry = probe.BuildRegistry();
		foreach (var stage in probeRegistry.AllStages) {
			services.TryAddKeyedScoped(stage.ServiceType, tenantKey);
		}

		EnsureKeyedInfrastructureRegistered(services, tenantKey);

		// Append configure в per-tenant accumulator.
		services.AddKeyedSingleton<IJobsConfigure>(tenantKey, new JobsConfigure(configure));

		return services;
	}

	private static void EnsureInfrastructureRegistered(IServiceCollection services) {
		// Проверка по StageRegistry: если он уже зарегистрирован — инфра поднята, ничего не делаем.
		if (services.Any(d => d.ServiceType == typeof(StageRegistry))) return;

		// Shared builder — singleton, переиспользуется всеми accumulator-элементами при сборке.
		services.AddSingleton<JobOrchestratorBuilder>();

		// Lazy StageRegistry: при первом resolve собирает все IJobsConfigure и применяет к shared builder.
		services.AddSingleton<StageRegistry>(sp => {
			var builder = sp.GetRequiredService<JobOrchestratorBuilder>();
			foreach (var c in sp.GetServices<IJobsConfigure>()) c.Apply(builder);
			return builder.BuildRegistry();
		});

		services.AddSingleton<InstanceManager>();
		services.AddSingleton<KeyspaceRegistry>();
		services.AddSingleton<ConcurrencyLimits>();
		services.AddSingleton<SuccessWaiters>();
		services.AddSingleton<OutcomeWaiters>();
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
	}

	/// <summary>
	/// Per-tenant keyed-infrastructure: регистрирует полный набор keyed-singleton'ов под
	/// <paramref name="tenantKey"/>. Однократная регистрация per tenant — повторные вызовы no-op.
	/// <para>
	/// Большинство keyed-singleton'ов регистрируется через <see cref="AddKeyedSingletonWithPropagation{T}"/>:
	/// ключ автоматически пробрасывается на конструкторные зависимости через
	/// <see cref="KeyedAwareServiceProvider"/>. Ручные factory остались только для случаев,
	/// которые ActivatorUtilities не покрывает: bootstrap-логика <see cref="StageRegistry"/>,
	/// фабрика <see cref="Channel.CreateBounded{T}(BoundedChannelOptions)"/> (нет конструктора).
	/// </para>
	/// </summary>
	private static void EnsureKeyedInfrastructureRegistered(IServiceCollection services, string tenantKey) {
		// Per-tenant маркер — наличие keyed StageRegistry под этим ключом означает, что инфра поднята.
		if (services.Any(d => d.ServiceType == typeof(StageRegistry) && d.IsKeyedService && Equals(d.ServiceKey, tenantKey))) return;

		// Shared per-tenant builder.
		services.AddKeyedSingleton<JobOrchestratorBuilder>(tenantKey);

		// Lazy keyed StageRegistry: собирает все per-tenant IJobsConfigure через GetKeyedServices.
		// Factory остаётся — это не простой конструкторный резолв, а bootstrap-логика.
		services.AddKeyedSingleton<StageRegistry>(tenantKey, (sp, key) => {
			var k = (string)key!;
			var builder = sp.GetRequiredKeyedService<JobOrchestratorBuilder>(k);
			foreach (var c in sp.GetKeyedServices<IJobsConfigure>(k)) c.Apply(builder);
			return builder.BuildRegistry();
		});

		// Per-tenant bounded channel — фабрика обязательна (у Channel нет конструктора).
		services.AddKeyedSingleton<Channel<OrchestratorEvent>>(tenantKey, (_, _) =>
			Channel.CreateBounded<OrchestratorEvent>(new BoundedChannelOptions(ChannelCapacity) {
				SingleReader = true,
				SingleWriter = false,
				FullMode = BoundedChannelFullMode.Wait,
			}));

		// Простые keyed-singleton'ы без keyed-зависимостей — дефолтный ActivatorUtilities-резолв.
		services.AddKeyedSingleton<InstanceManager>(tenantKey);
		services.AddKeyedSingleton<KeyspaceRegistry>(tenantKey);
		services.AddKeyedSingleton<SuccessWaiters>(tenantKey);
		services.AddKeyedSingleton<OutcomeWaiters>(tenantKey);

		// Keyed-сервисы с keyed-зависимостями — авто-пропагация ключа через wrapper.
		services.AddKeyedSingletonWithPropagation<ConcurrencyLimits>(tenantKey);
		services.AddKeyedSingletonWithPropagation<OrchestratorLifecycle>(tenantKey);
		services.AddKeyedSingletonWithPropagation<InstanceCreator>(tenantKey);
		services.AddKeyedSingletonWithPropagation<StageRunner>(tenantKey);
		services.AddKeyedSingletonWithPropagation<DueScanner>(tenantKey);
		services.AddKeyedSingletonWithPropagation<EventLoop>(tenantKey);
		services.AddKeyedSingletonWithPropagation<IJobOrchestrator, JobOrchestratorRuntime>(tenantKey);

		// HostedService регистрируется как IHostedService (host стартует все зарегистрированные) —
		// поэтому это non-keyed registration, но внутри использует тот же wrapper для авто-пропагации.
		services.AddSingleton<IHostedService>(sp =>
			ActivatorUtilities.CreateInstance<JobOrchestratorHostedService>(
				new KeyedAwareServiceProvider(sp, tenantKey)));

		// Shared infrastructure: TimeProvider может быть уже зарегистрирован хостом.
		services.TryAddSingleton(TimeProvider.System);
	}

	/// <summary>
	/// Регистрирует <typeparamref name="T"/> как keyed-singleton под <paramref name="key"/>,
	/// автоматически пробрасывая этот же ключ при резолве keyed-зависимостей в конструкторе.
	/// Non-keyed-зависимости (<see cref="TimeProvider"/>, <see cref="ILogger{T}"/>, и т.п.)
	/// резолвятся как обычно через root-контейнер.
	/// </summary>
	private static IServiceCollection AddKeyedSingletonWithPropagation<T>(
		this IServiceCollection services, object key) where T : class =>
		services.AddKeyedSingleton<T>(key, (sp, k) =>
			ActivatorUtilities.CreateInstance<T>(new KeyedAwareServiceProvider(sp, k!)));

	/// <summary>
	/// Регистрирует <typeparamref name="TImpl"/> как реализацию <typeparamref name="TService"/>
	/// в keyed-singleton под <paramref name="key"/>; ключ авто-пробрасывается на конструкторные
	/// зависимости через <see cref="KeyedAwareServiceProvider"/>.
	/// </summary>
	private static IServiceCollection AddKeyedSingletonWithPropagation<TService, TImpl>(
		this IServiceCollection services, object key)
		where TService : class
		where TImpl : class, TService =>
		services.AddKeyedSingleton<TService>(key, (sp, k) =>
			ActivatorUtilities.CreateInstance<TImpl>(new KeyedAwareServiceProvider(sp, k!)));
}
