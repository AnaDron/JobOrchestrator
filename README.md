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

`ProducerService` через `ctx.AddKey("k1")` регистрирует ключ — SDK немедленно создаёт инстанс `consumer[producer=k1]` и ставит в очередь. Подробности — в `docs/ultrathink-dynamic-emerson.md`.

## Сборка и тесты

```
dotnet build libs/job-orchestrator/JobOrchestrator.slnx
dotnet test libs/job-orchestrator/test/JobOrchestrator.Tests
dotnet test libs/job-orchestrator/test/JobOrchestrator.IntegrationTests
```
