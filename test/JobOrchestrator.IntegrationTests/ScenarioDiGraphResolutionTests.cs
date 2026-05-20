using System.Threading.Channels;
using JobOrchestrator.IntegrationTests.Support;
using JobOrchestrator.Internal;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Smoke-тесты: проверяют, что DI-граф SDK резолвится end-to-end под всеми поддерживаемыми
/// конфигурациями (unkeyed / single-keyed-tenant / multi-keyed-tenant) без runtime-ошибок.
/// <para>
/// <b>Зачем нужны:</b> регистрации в <see cref="ServiceCollectionExtensions"/> для keyed-режима
/// используют <see cref="ActivatorUtilities.CreateInstance"/> через
/// <see cref="KeyedAwareServiceProvider"/> — ошибка в конструкторе любого внутреннего сервиса
/// (например, добавили новую зависимость и забыли её зарегистрировать) проявится только при
/// первом resolve этого типа. Эти тесты резолвят весь корневой граф эксплицитно, чтобы
/// поймать такую регрессию на CI, а не на первом запуске tenant-job в production.
/// </para>
/// <para>
/// <b>Покрытие через листья:</b> <see cref="EventLoop"/> и <see cref="JobOrchestratorRuntime"/>
/// — самые «жирные» listed types (по числу транзитивных deps). Их успешный resolve гарантирует,
/// что все нижестоящие сервисы тоже сконструировались успешно. Дополнительно резолвим
/// <see cref="IHostedService"/>-коллекцию — это вход host'а, и через неё тоже идёт типовая
/// production-резолюция.
/// </para>
/// </summary>
public sealed class ScenarioDiGraphResolutionTests {
	private static IHost Build(Action<IServiceCollection> register) {
		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();
		register(builder.Services);
		return builder.Build();
	}

	[Fact]
	public void Unkeyed_RootGraph_Resolves() {
		using var host = Build(s => {
			s.AddJobOrchestrator(jobs => {
				jobs.UseInMemoryStateStore();
				jobs.Stage("solo")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			});
		});

		// Каждый Action в списке — Invoking-проверка: ловим первое же исключение в графе.
		var checks = new (string Name, Action Resolve)[] {
			(nameof(JobOrchestratorBuilder), () => _ = host.Services.GetRequiredService<JobOrchestratorBuilder>()),
			(nameof(StageRegistry),          () => _ = host.Services.GetRequiredService<StageRegistry>()),
			(nameof(InstanceManager),        () => _ = host.Services.GetRequiredService<InstanceManager>()),
			(nameof(KeyspaceRegistry),       () => _ = host.Services.GetRequiredService<KeyspaceRegistry>()),
			(nameof(SuccessWaiters),         () => _ = host.Services.GetRequiredService<SuccessWaiters>()),
			(nameof(ConcurrencyLimits),      () => _ = host.Services.GetRequiredService<ConcurrencyLimits>()),
			(nameof(GlobalIterationLimiter),  () => _ = host.Services.GetRequiredService<GlobalIterationLimiter>()),
			(nameof(OrchestratorLifecycle),  () => _ = host.Services.GetRequiredService<OrchestratorLifecycle>()),
			(nameof(InstanceCreator),        () => _ = host.Services.GetRequiredService<InstanceCreator>()),
			(nameof(StageRunner),            () => _ = host.Services.GetRequiredService<StageRunner>()),
			(nameof(DueScanner),             () => _ = host.Services.GetRequiredService<DueScanner>()),
			(nameof(EventLoop),              () => _ = host.Services.GetRequiredService<EventLoop>()),
			(nameof(IJobOrchestrator),       () => _ = host.Services.GetRequiredService<IJobOrchestrator>()),
			("Channel<OrchestratorEvent>",   () => _ = host.Services.GetRequiredService<Channel<OrchestratorEvent>>()),
			("IHostedService[]",             () => _ = host.Services.GetServices<IHostedService>().ToList()),
		};

		foreach (var (name, resolve) in checks) {
			resolve.Should().NotThrow(because: $"unkeyed: {name} обязан резолвиться без ошибок");
		}
	}

	[Fact]
	public void SingleTenant_KeyedRootGraph_Resolves() {
		const string tenant = "evotor";
		using var host = Build(s => {
			s.AddJobOrchestrator(tenant, jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain(tenant, evotor =>
					evotor.Stage("shops")
						.HandledBy<FakeServiceA>()
						.RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			// Stage-service подменяем keyed-singleton'ом, чтобы probe-pass TryAddKeyedScoped
			// был перекрыт нашей фиксированной реализацией (см. ScenarioKeyedMultiTenancyTests).
			s.AddKeyedSingleton<FakeServiceA>(tenant, new FakeServiceA());
		});

		var checks = new (string Name, Action Resolve)[] {
			(nameof(JobOrchestratorBuilder), () => _ = host.Services.GetRequiredKeyedService<JobOrchestratorBuilder>(tenant)),
			(nameof(StageRegistry),          () => _ = host.Services.GetRequiredKeyedService<StageRegistry>(tenant)),
			(nameof(InstanceManager),        () => _ = host.Services.GetRequiredKeyedService<InstanceManager>(tenant)),
			(nameof(KeyspaceRegistry),       () => _ = host.Services.GetRequiredKeyedService<KeyspaceRegistry>(tenant)),
			(nameof(SuccessWaiters),         () => _ = host.Services.GetRequiredKeyedService<SuccessWaiters>(tenant)),
			(nameof(ConcurrencyLimits),      () => _ = host.Services.GetRequiredKeyedService<ConcurrencyLimits>(tenant)),
			(nameof(GlobalIterationLimiter),  () => _ = host.Services.GetRequiredKeyedService<GlobalIterationLimiter>(tenant)),
			(nameof(OrchestratorLifecycle),  () => _ = host.Services.GetRequiredKeyedService<OrchestratorLifecycle>(tenant)),
			(nameof(InstanceCreator),        () => _ = host.Services.GetRequiredKeyedService<InstanceCreator>(tenant)),
			(nameof(StageRunner),            () => _ = host.Services.GetRequiredKeyedService<StageRunner>(tenant)),
			(nameof(DueScanner),             () => _ = host.Services.GetRequiredKeyedService<DueScanner>(tenant)),
			(nameof(EventLoop),              () => _ = host.Services.GetRequiredKeyedService<EventLoop>(tenant)),
			(nameof(IJobOrchestrator),       () => _ = host.Services.GetRequiredKeyedService<IJobOrchestrator>(tenant)),
			("Channel<OrchestratorEvent>",   () => _ = host.Services.GetRequiredKeyedService<Channel<OrchestratorEvent>>(tenant)),
			("IHostedService[]",             () => _ = host.Services.GetServices<IHostedService>().ToList()),
		};

		foreach (var (name, resolve) in checks) {
			resolve.Should().NotThrow(because: $"keyed[{tenant}]: {name} обязан резолвиться без ошибок");
		}
	}

	[Fact]
	public void MultiTenant_BothGraphsResolveIndependently() {
		using var host = Build(s => {
			s.AddJobOrchestrator("evotor", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("evotor", evotor =>
					evotor.Stage("shops")
						.HandledBy<FakeServiceA>()
						.RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddJobOrchestrator("ozon", jobs => {
				jobs.UseInMemoryStateStore();
				jobs.WithDomain("ozon", ozon =>
					ozon.Stage("offers")
						.HandledBy<FakeServiceB>()
						.RunPeriodically(TimeSpan.FromMinutes(1)));
			});
			s.AddKeyedSingleton<FakeServiceA>("evotor", new FakeServiceA());
			s.AddKeyedSingleton<FakeServiceB>("ozon", new FakeServiceB());
		});

		// Резолвим оба leaf-graph'а независимо — failure одного tenant'а не должен прятать failure другого.
		foreach (var tenant in new[] { "evotor", "ozon" }) {
			((Action)(() => _ = host.Services.GetRequiredKeyedService<EventLoop>(tenant)))
				.Should().NotThrow(because: $"keyed[{tenant}]: EventLoop тянет за собой весь tenant-граф");
			((Action)(() => _ = host.Services.GetRequiredKeyedService<IJobOrchestrator>(tenant)))
				.Should().NotThrow(because: $"keyed[{tenant}]: IJobOrchestrator — публичная точка входа");
		}

		// IHostedService-коллекция должна содержать ОБА per-tenant хост-сервиса (один на тенант).
		var hostedServices = host.Services.GetServices<IHostedService>().ToList();
		hostedServices.OfType<JobOrchestratorHostedService>().Should().HaveCount(2,
			because: "каждый AddJobOrchestrator(tenantKey, ...) добавляет свой IHostedService");
	}
}
