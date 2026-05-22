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

Внешние операции идут через трёхуровневую иерархию: `IJobOrchestrator → IDomainHandle → IStageHandle → IInstanceHandle → IIterationHandle`. Для бездоменных стадий — `orchestrator.Root["stage"]`; для доменных — `orchestrator["domain"]["stage"]`.

```csharp
// Bootstrap keyspace для keyless-эмитера:
orchestrator.Root["producer"].RegisterKey("k1");
orchestrator.Root["producer"].UnregisterKey("k1");

// Ручной запуск инстанса — implicit-конверсия tuple → InstanceKeys:
var iteration = await orchestrator.Root["consumer"][("producer", "k1")].RunAsync(ct);
// На любой отказ запуска (Debounced / ConcurrencyDeferred / NotFound / Terminating / AlreadyRunning /
// Faulted) бросается IterationRejectedException с конкретным Reason.

// Дождаться завершения — стандартный Task API; success = тихий await, failure = exception:
try {
    await iteration.Completion;
    // успех
} catch (IterationFailedException ex) {
    switch (ex.Reason) {
        case IterationFailureReason.StageException: /* stage handler бросил; ex.InnerException = причина */ break;
        case IterationFailureReason.Cancelled:      /* инстанс удалён каскадом */ break;
        case IterationFailureReason.Faulted:        /* оркестратор остановлен / event-loop crash */ break;
    }
}

// Composability — Task.WhenAll/Any, ContinueWith, WaitAsync(ct) — всё стандартно:
await iteration.Completion.WaitAsync(TimeSpan.FromSeconds(10));

// Состояние / диагностика конкретного инстанса:
var handle = orchestrator.Root["consumer"][("producer", "k1")];
var state = handle.State;
var snapshot = handle.Snapshot;
var running = handle.RunningIteration;   // активная сейчас итерация или null

// Ожидание первого успеха (memoized по identity):
await handle.WaitForSuccessAsync(ct);

// Итерация всех материализованных инстансов одной стадии (IStageHandle : IReadOnlyCollection<IInstanceHandle>):
foreach (var h in orchestrator.Root["consumer"]) {
    Console.WriteLine($"{h.FullyQualifiedName}: {h.State}");
}

// Итерация всех доменов (IJobOrchestrator : IReadOnlyCollection<IDomainHandle>):
foreach (var domain in orchestrator) {
    Console.WriteLine($"{(domain.Name.Length == 0 ? "<root>" : domain.Name)}: {domain.Count} стадий");
}

// Lookup стадии по полному имени (для конфигурации/логов):
var stage = orchestrator.GetStage("evotor:shops");
var all = orchestrator.AllStages();  // плоский срез по всем доменам
```

**Composite keys** (2+ компонента) — явный `new InstanceKeys` либо implicit-конверсия из `ReadOnlySpan`:
```csharp
await orchestrator.Root["docs"][new InstanceKeys(("shops", "u1"), ("currencies", "USD"))].RunAsync();
```

**Observable life-cycle keyspace** — `IStageHandle.Changes` отдаёт replay существующих + live-поток `StageChange(Added/Removed)`:
```csharp
await foreach (var change in orchestrator["evotor"]["shops"].Changes.WithCancellation(ct)) {
    Console.WriteLine($"{change.Kind}: {change.Instance.FullyQualifiedName}");
}
```

**Поток итераций инстанса** — `IInstanceHandle : IAsyncEnumerable<IIterationHandle>`:
```csharp
await foreach (var iter in orchestrator.Root["consumer"][("producer", "k1")].WithCancellation(ct)) {
    await iter.Completion;
    // ...
}
```

**Кэширование handles в hot-path**: indexer-вызовы создают per-call аллокации. Для polling-сценариев кэшируйте handle локально:
```csharp
var h = orchestrator.Root["consumer"][("producer", "k1")];  // один раз
while (running) {
    var state = h.State;                                     // re-uses cached identity
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
- **Backpressure** — очередь bounded (10 000), единый `ChannelWriterExtensions` (fast `TryWrite` / slow `WriteAsync`). Внутри `IJobService` и `RunAsync` — `PublishAsync` (async). Sync-`Publish` — completion runner-а, DueScanner, `RegisterKey`/`UnregisterKey`; не с HTTP-request thread.
- **Глобальный лимит** — `jobs.Defaults.GlobalConcurrencyLimit = N` ограничивает суммарный параллелизм итераций поверх `WithConcurrencyLimit` per-stage.
- **`RunAsync`** — manual-запуск с возвратом `IIterationHandle` конкретной итерации. Handle привязан к ЗАПУЩЕННОМУ циклу (не identity), поэтому race с Auto-тиками невозможен. Acceptance-reject (Debounced/ConcurrencyDeferred/NotFound/Terminating/AlreadyRunning/Faulted) — `IterationRejectedException(Reason)`. Завершение через `iteration.Completion` (`Task`): тихий `await` = успех; `IterationFailedException(Reason)` = не-успешный исход (`StageException` — бизнес-ошибка handler'а, оригинал в `InnerException`; `Cancelled` — каскадная отмена; `Faulted` — shutdown / event-loop crash).
- **Устойчивость** — `ManualTrigger` TCS при сбое handler'а; мутирующие события (ключи/completion/tick) → fault сразу (`FaultOnStateMutatingHandlerCrash`, default on). Иначе fault после N подряд. `ConcurrencyDeferJitterMaxMilliseconds` (default 500) при лимитах. `ShutdownIterationTimeout` — принудительный cancel итераций после shutdown.

## Сборка и тесты

```
dotnet build libs/job-orchestrator/JobOrchestrator.slnx
dotnet test libs/job-orchestrator/test/JobOrchestrator.Tests
dotnet test libs/job-orchestrator/test/JobOrchestrator.IntegrationTests
```
