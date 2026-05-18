# Job Orchestrator SDK

Инфраструктурный SDK для оркестрации стадий с зависимостями: расписание, retry/debounce/watchdog, ручные триггеры, потоковая параметризация инстансов через keyspace и каскадное наследование ключей по графу зависимостей.

SDK независим от какой-либо доменной области — это переиспользуемая библиотека внутри monorepo Aura, развязанная от Aura-кода и готовая к экстракции в самостоятельный repo / публикации в NuGet.

## Структура

```
libs/job-orchestrator/
├── JobOrchestrator.slnx
├── Directory.Build.props
├── README.md
├── TODO.md                    # SDK-специфичные отложенные пункты
├── docs/                      # архитектурные доки
├── src/
│   ├── JobOrchestrator/       # основной SDK
│   └── JobOrchestrator.InMemory/  # дефолтный backend IJobStateStore
├── test/
│   ├── JobOrchestrator.Tests/
│   └── JobOrchestrator.IntegrationTests/
└── sample/
    └── JobOrchestrator.Sample/
```

## Использование (минимальный пример)

```csharp
services.AddJobOrchestrator(jobs => {
    var producer = jobs.Stage("producer")
        .HandledBy<ProducerService>()
        .RunPeriodically(TimeSpan.FromMinutes(5));

    var consumer = jobs.Stage("consumer")
        .HandledBy<ConsumerService>()
        .DependsOnInstance(producer)
        .RunPeriodically(TimeSpan.FromMinutes(1));
});
services.AddInMemoryJobStateStore();
```

`ProducerService` через `ctx.AddKey("k1")` регистрирует ключ — SDK немедленно создаёт инстанс `consumer[producer=k1]` и ставит в очередь.

## Handle-API

Внешние операции (manual trigger, bootstrap keyspace, диагностика, wait) идут через композицию индексаторов:

```csharp
// Bootstrap keyspace для keyless-эмитера:
orchestrator["producer"].RegisterKey("k1");
orchestrator["producer"].UnregisterKey("k1");

// Ручной trigger:
await orchestrator["producer"][InstanceKey.None].TriggerAsync();
await orchestrator["consumer"][("producer", "k1")].TriggerAsync();

// Состояние / диагностика конкретного инстанса:
var state = orchestrator["consumer"][("producer", "k1")].State;
var snapshot = orchestrator["consumer"][("producer", "k1")].Snapshot;

// Ожидание исхода:
await orchestrator["consumer"][("producer", "k1")].WaitForSuccessAsync(ct);
var outcome = await orchestrator["consumer"][("producer", "k1")].WaitForOutcomeAsync(ct);

// Итерация всех инстансов одной стадии:
foreach (var h in orchestrator["consumer"].AllInstances) {
    Console.WriteLine($"{h.FullyQualifiedName}: {h.State}");
}

// Итерация всех зарегистрированных стадий (IJobOrchestrator : IEnumerable<IStageHandle>):
foreach (var stage in orchestrator) { /* ... */ }
```

**Composite keys** (2+ компонента) — через tuple-индексатор или span-params:
```csharp
orchestrator["docs"][("shops", "u1"), ("currencies", "USD")].TriggerAsync();
```

**Кэширование handles в hot-path**: indexer-вызовы создают per-call аллокации. Для polling-сценариев кэшируйте handle локально:
```csharp
var h = orchestrator["consumer"][("producer", "k1")];  // один раз
while (running) {
    var state = h.State;                                 // re-uses cached identity
    await Task.Delay(...);
}
```

## Концепции

SDK оперирует тремя сущностями: **Stage** (immutable декларация в Fluent API), **StageInstance** (long-lived runtime-экземпляр с композитным ключом), **Job** (одна итерация = один вызов `IJobService.ExecuteAsync`). Соотношение `Stage:Instance = 1:N`, `Instance:Job = 1:M`. Подробный разбор и иллюстрация на Эвотор-сценарии — в [docs/concepts.md](docs/concepts.md).

## Архитектура

Описание устройства SDK (single-threaded event loop, dynamic-delay DueScanner, каскадное удаление с deferred cleanup, backtracking-merge при создании инстансов, lock-free `GetOverview()` через atomic-fields, lifecycle/IsFaulted, структурное логирование) — в [docs/architecture.md](docs/architecture.md).

### Эксплуатация (Aura)

- **Один оркестратор на процесс** — граф, keyspace и waiters in-memory; горизонтальное масштабирование нескольких writer'ов без внешнего lease не поддерживается (см. `TODO.md`).
- **Рестарт процесса** — граф стадий, keyspace и метрики инстансов в RAM; после рестарта нужен bootstrap (`RegisterKey`, первые sync-итерации). Персистентность — через `IJobState` / `IJobStateStore`.
- **Backpressure** — очередь bounded (10 000), единый `ChannelWriterExtensions` (fast `TryWrite` / slow `WriteAsync`). Внутри `IJobService` и `TriggerAsync` — `PublishAsync` (async). Sync-`Publish` — completion runner-а, DueScanner, `RegisterKey`/`UnregisterKey`; не с HTTP-request thread.
- **Глобальный лимит** — `jobs.Defaults.GlobalConcurrencyLimit = N` ограничивает суммарный параллелизм итераций поверх `WithConcurrencyLimit` per-stage.
- **`WaitForOutcomeAsync`** — мемоизирует последний исход (Success/Failure/Cancelled): если итерация уже завершилась, Task резолвится сразу с этим outcome. Чтобы дождаться конкретного запуска — сначала `WaitForOutcomeAsync()`, затем `TriggerAsync()` (см. XML на `IInstanceHandle`).
- **Устойчивость** — сбой handler'а event loop: `ManualTrigger` TCS завершается исключением; после `HandlerCrashFaultThreshold` подряд (по умолчанию 3) — `MarkFaulted`. Опционально `ConfigureJobOrchestratorHost(o => o.ShutdownIterationTimeout = …)` форсирует cancel running-итераций после graceful shutdown.

## Сборка и тесты

```
dotnet build libs/job-orchestrator/JobOrchestrator.slnx
dotnet test libs/job-orchestrator/test/JobOrchestrator.Tests
dotnet test libs/job-orchestrator/test/JobOrchestrator.IntegrationTests
```
