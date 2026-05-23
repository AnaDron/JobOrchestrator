using System.Threading.Channels;
using JobOrchestrator.Configuration;
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
	/// Требует, чтобы в configure-action был вызван хотя бы один backend-extension для <see cref="IJobStateStore"/> —
	/// например, <c>jobs.UseInMemoryStateStore()</c> (пакет <c>JobOrchestrator.InMemory</c>) или аналог из
	/// внешнего backend-пакета. Отсутствие регистрации диагностируется fail-fast при первом resolve
	/// <see cref="Internal.StageRegistry"/>.
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
			var registry = builder.BuildRegistry();
			EnsureStateStoreRegistered(sp, tenantKey: null);
			return registry;
		});

		services.AddSingleton<InstanceManager>();
		services.AddSingleton<ConcurrencyLimits>();
		services.AddSingleton<GlobalIterationLimiter>();
		services.AddSingleton<InstanceCreator>();
		services.AddSingleton<StageRunner>();
		services.AddSingleton<DueScanner>();
		services.AddSingleton<EventLoop>();

		// Bounded channel: backpressure через Wait. При заполнении внешние писатели (Manual triggers,
		// RegisterKey) подождут места; event loop как consumer обычно их быстро разгружает.
		services.AddSingleton(_ => CreateEventChannel());

		services.AddSingleton<OrchestratorLifecycle>();
		services.TryAddSingleton<JobOrchestratorHostOptions>();
		// JobOrchestratorRuntime регистрируется concrete'но, потому что EventLoop принимает его типом
		// (для конструирования IterationHandle с reverse-link на Instance). Interface IJobOrchestrator —
		// alias на тот же singleton.
		services.AddSingleton<JobOrchestratorRuntime>();
		services.AddSingleton<IJobOrchestrator>(sp => sp.GetRequiredService<JobOrchestratorRuntime>());
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
			var registry = builder.BuildRegistry();
			EnsureStateStoreRegistered(sp, tenantKey: k);
			return registry;
		});

		// Per-tenant bounded channel — фабрика обязательна (у Channel нет конструктора).
		services.AddKeyedSingleton(tenantKey, (_, _) => CreateEventChannel());

		// Простые keyed-singleton'ы без keyed-зависимостей — дефолтный ActivatorUtilities-резолв.
		services.AddKeyedSingleton<InstanceManager>(tenantKey);

		// Per-tenant дефолтный HostOptions: keyed-singleton под tenantKey. Пользовательский override
		// через jobs.ConfigureJobOrchestratorHost(...) уже зарегистрирован на probe-pass ДО этой
		// строки — TryAdd-семантика делает дефолт no-op в этом случае. Без tenant'а tenants делили бы
		// один экземпляр HostOptions, нарушая инфраструктурную изоляцию.
		services.TryAddKeyedSingleton<JobOrchestratorHostOptions>(tenantKey, (_, _) => new JobOrchestratorHostOptions());

		// Keyed-сервисы с keyed-зависимостями — авто-пропагация ключа через wrapper.
		services.AddKeyedSingletonWithPropagation<ConcurrencyLimits>(tenantKey);
		services.AddKeyedSingletonWithPropagation<GlobalIterationLimiter>(tenantKey);
		services.AddKeyedSingletonWithPropagation<OrchestratorLifecycle>(tenantKey);
		services.AddKeyedSingletonWithPropagation<InstanceCreator>(tenantKey);
		services.AddKeyedSingletonWithPropagation<StageRunner>(tenantKey);
		services.AddKeyedSingletonWithPropagation<DueScanner>(tenantKey);
		services.AddKeyedSingletonWithPropagation<EventLoop>(tenantKey);
		services.AddKeyedSingletonWithPropagation<JobOrchestratorRuntime>(tenantKey);
		services.AddKeyedSingleton<IJobOrchestrator>(tenantKey,
			(sp, key) => sp.GetRequiredKeyedService<JobOrchestratorRuntime>(key!));

		// HostedService регистрируется как IHostedService (host стартует все зарегистрированные) —
		// поэтому это non-keyed registration, но внутри использует тот же wrapper для авто-пропагации.
		services.AddSingleton<IHostedService>(sp =>
			ActivatorUtilities.CreateInstance<JobOrchestratorHostedService>(
				new KeyedAwareServiceProvider(sp, tenantKey)));

		// Shared infrastructure: TimeProvider может быть уже зарегистрирован хостом.
		services.TryAddSingleton(TimeProvider.System);
	}

	private static Channel<OrchestratorEvent> CreateEventChannel() =>
		Channel.CreateBounded<OrchestratorEvent>(new BoundedChannelOptions(ChannelCapacity) {
			SingleReader = true,
			SingleWriter = false,
			FullMode = BoundedChannelFullMode.Wait,
		});

	/// <summary>
	/// Fail-fast диагностика для забытого <c>jobs.UseInMemoryStateStore()</c> (или аналога из
	/// внешнего backend-пакета). Вызывается из <see cref="StageRegistry"/>-factory — момент, когда
	/// все configure-action'ы уже выполнились (включая multi-call accumulation), и можно надёжно
	/// проверить итоговый набор регистраций.
	/// <para>
	/// Для tenant'ового orchestrator-а ищем <b>именно</b> keyed-descriptor под <paramref name="tenantKey"/>:
	/// fallback на non-keyed singleton <c>IJobStateStore</c> через <see cref="Keyed.KeyedAwareServiceProvider"/>
	/// технически работал бы, но нарушил бы tenant-изоляцию (общий store на нескольких tenant'ов с
	/// возможным cross-contamination по <see cref="Instance.StateScope"/>). Требуем явный keyed-store
	/// — это симметрично контракту «tenant — полная изоляция инфраструктуры».
	/// </para>
	/// </summary>
	private static void EnsureStateStoreRegistered(IServiceProvider sp, string? tenantKey) {
		IJobStateStore? store = tenantKey is null
			? sp.GetService<IJobStateStore>()
			: sp.GetKeyedService<IJobStateStore>(tenantKey);
		if (store is not null) return;
		string qualifier = tenantKey is null
			? "AddJobOrchestrator(...)"
			: $"AddJobOrchestrator(\"{tenantKey}\", ...)";
		string hint = tenantKey is null
			? "jobs.UseInMemoryStateStore() (или аналог из backend-пакета)"
			: $"jobs.UseInMemoryStateStore() (или аналог) внутри configure-action — backend поднимется как keyed-singleton под '{tenantKey}'";
		throw new InvalidOperationException(
			$"Для {qualifier} не зарегистрирован IJobStateStore. Вызовите {hint}.");
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

}
