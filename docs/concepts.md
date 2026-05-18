# Концепции SDK

Job Orchestrator оперирует тремя смысловыми сущностями: **Stage**, **Instance** и **Job (итерация)**. Их разделение — фундаментальное; без него невозможен fan-out по динамическому набору ключей. Этот документ объясняет каждую сущность, их соотношение и то, как они называются в публичном и внутреннем API.

## Триада «декларация → экземпляр → итерация»

```
        ┌───────────────────────────────┐
        │ Stage  (декларация в Fluent)   │  immutable, один на имя
        │  Name, IJobService, Interval,  │
        │  RetryPolicy, Debounce,        │
        │  ExecutionTimeout, Dependencies│
        └───────────────────────────────┘
                       │  1 : N
                       ▼
        ┌───────────────────────────────┐
        │ Instance (runtime-экз.)        │  long-lived, по одному на
        │  Stage, DependencyKeys,        │  ключевую комбинацию
        │  LastSuccess, LastAttempt,     │
        │  ConsecutiveFailures, RunCts   │
        └───────────────────────────────┘
                       │  1 : M
                       ▼
        ┌───────────────────────────────┐
        │ Job  (одна итерация)           │  кратковременная,
        │  IJobService.ExecuteAsync(ctx) │  один вызов = один Job
        │  + DI-scope + watchdog CTS     │
        └───────────────────────────────┘
```

Цифры: `Stage:Instance = 1:N`, `Instance:Job = 1:M`. На один Stage может быть от 0 до тысяч `Instance`, и каждый из них переживает множество итераций.

## Stage — декларация

**Что это.** Описание стадии, заданное в Fluent API через `JobOrchestratorBuilder.Stage(name)`:

```csharp
var products = jobs.Stage("products")
    .HandledBy<ProductsService>()
    .DependsOnInstance(shops)
    .RunPeriodically(TimeSpan.FromMinutes(5))
    .RetryAfterFailure(RetryPolicy.ExponentialBackoff(...))
    .WithExecutionTimeout(TimeSpan.FromMinutes(15));
```

**Свойства:**
- Уникальное имя.
- Тип реализации (`IJobService`).
- Расписание, retry-policy, debounce, watchdog.
- Список зависимостей (`DependsOn` + `DependsOnInstance`).

**Контракт жизненного цикла:**
- Создаётся **один раз** при `services.AddJobOrchestrator(...)`.
- **Immutable** после `BuildRegistry()` — больше не меняется до останова процесса.
- Существует **один** объект на каждое уникальное имя; все runtime-экземпляры стадии держат на него ссылку.

**Где в коде:**
- Публично: `IStageBuilder` (Configuration), `Stage(name)` в `JobOrchestratorBuilder`.
- Внутренне: `StageDescriptor` (`Internal/StageDescriptor.cs`) — финальный immutable снимок после сборки. Все `StageDescriptor` собраны в `StageRegistry` (`Internal/StageRegistry.cs`).

## Instance — runtime-экземпляр

**Что это.** Конкретный исполнительный экземпляр стадии с уникальным композитным ключом (`DependencyKeys`). Существует столько `Instance`-ов, сколько комбинаций ключей удовлетворяет зависимостям стадии.

Например, при `productGroups.DependsOnInstance(shops)` и keyspace `shops = {u1, u2, u3}` существует ровно три инстанса:
- `productGroups[shops=u1]`
- `productGroups[shops=u2]`
- `productGroups[shops=u3]`

**Свойства:**
- Ссылка на свой `Stage`.
- `DependencyKeys: IReadOnlyDictionary<string, string>` — композитный ключ.
- `FullyQualifiedName` — каноническое имя для логирования (`"productGroups[shops=u1]"`).
- `EncodedKey` — каноническая строка для словарного ключа в `InstanceManager` (sort by name).
- `StateScope` — scope для `IJobStateStore` (`"{StageName}:{EncodedKey}"`).
- Runtime-state: `State` (`Idle`/`Running`), `LastSuccess` (монотонная), `LastAttempt`, `ConsecutiveFailures`, `LastError`, `NextTickAtMs`, `RunCts`.

**Контракт жизненного цикла:**
- Создаётся **динамически** через `InstanceCreator.EvaluateAndCreate(stage)` когда:
  - все `DependsOnInstance(X)` зависимости имеют нужные ключи в keyspace(X) + парный X-инстанс успешен (`LastSuccess != null`);
  - все `DependsOn(Y)` зависимости имеют парный успешный Y-инстанс.
- Удаляется через каскад `RemoveKeyAsync`/`UnregisterKey` — графически вниз по транзитивному замыканию зависимостей, в топологически обратном порядке (листья перед корнями).
- **Mutable runtime-state** — изменяется единственным consumer-потоком event loop'а (single-threaded), поэтому без блокировок и без CAS.
- Существует от момента создания до удаления; переживает множество итераций.

**Где в коде:**
- Внутренне: `Instance` (`Internal/Instance.cs`); коллекция в `InstanceManager` (`Internal/InstanceManager.cs`).
- Снаружи через диагностику: `InstanceInfo` (`Abstractions/InstanceInfo.cs`) — снапшот одного `Instance` для `InstancesOverview` (`Abstractions/InstancesOverview.cs`).

**Pending состояния нет** — инстанс физически не существует до момента разрешения зависимостей. Это упрощает state machine: только `Idle`/`Running` (см. `InstanceLifecycleState`).

## Job — итерация

**Что это.** Одна материализация работы — один вызов `IJobService.ExecuteAsync(JobContext ctx, CancellationToken ct)` для конкретного `Instance`. В коде это **не отдельный тип**, а просто scope + context для одного вызова. Время жизни — от запуска до завершения метода.

**Триггеры запуска:**
- `Auto` — internal timer тикает по `Instance.NextTickAtMs`.
- `Manual` — `orchestrator["stageName"][(keyName, keyValue), ...].TriggerAsync()`.

**Что происходит вокруг одной итерации:**
1. EventLoop принимает решение `TryAccept(instance, source)` — проверяет `Running`/`AlreadyRunning`, retry-delay (для Auto), debounce (для Manual).
2. Переход `instance.State = Running`.
3. `StageRunner.RunIterationAsync` создаёт fresh DI-scope, watchdog-CTS, формирует `JobContext`, кладёт в `logger.BeginScope` структурные поля (`FullyQualifiedName`, `StageName`, `{depStage}Key`).
4. `service.ExecuteAsync(ctx, ct)` — пользовательский код. Может вызывать `ctx.AddKeyAsync/RemoveKeyAsync` любое число раз, может работать с `ctx.State` (`IJobState`), читает `ctx.LastSuccessAt`, `ctx.DependencyKeys`.
5. По завершении публикуется `StageCompletedEvent` (если без исключения) или `StageFailedEvent`.
6. EventLoop обновляет `instance.LastAttempt`, `LastSuccess` / `ConsecutiveFailures`, и перепланирует следующий тик.

**Где в коде это «нейминг»:**
- Публично: `IJobService.ExecuteAsync` — выполняет одну итерацию. `JobContext` — контекст этой итерации. `InstanceInfo.StageName` / `FullyQualifiedName` — описывает инстанс, в чью «биографию» эта итерация попадёт.
- Внутренне явного типа `Job` нет. `StageRunner.RunIterationAsync(Instance instance, ...)` принимает инстанс и проигрывает одну итерацию.

## Двойственность слова Job

В разных планировщиках слово «job» используется по-разному. У нас две согласованных трактовки сосуществуют:

| Слово | В нашем SDK означает |
|---|---|
| `IJobService.ExecuteAsync` | «job» = одна итерация (одна материализация работы) |
| `JobContext` | «контекст job-а» = контекст итерации |
| `InstanceInfo` / `InstancesOverview` | view-модель long-lived state одного `Instance` (см. § «Где в коде» в разделе Instance) |
| `IJobOrchestrator` | «orchestrator of jobs» = общая фраза, контракт управления |
| `JobOrchestratorBuilder` / `JobConfigurationException` / `JobOrchestratorHostedService` / `JobDefaults` / `IJobState` / `IJobStateStore` | широкая агрегированная семантика — «всё, что касается job-ов» |

Внутренний рантайм-объект называется `Instance` (не `Job`), потому что long-lived runtime-сущность с mutable state — это **экземпляр стадии**, не «job». Это разделение проще всего запомнить через два правила:
- В public API слово «Job» = «work unit» в самом общем смысле (итерация, конфигурация, контракт).
- Во внутреннем коде («что я держу в `Dictionary`?») — это `Instance`.

## Эвотор — иллюстрация на полном графе

```csharp
var shops = jobs.Stage("shops").HandledBy<ShopsService>()
    .RunPeriodically(TimeSpan.FromHours(24));

var employees = jobs.Stage("employees").HandledBy<EmployeesService>()
    .DependsOn(shops)
    .RunPeriodically(TimeSpan.FromMinutes(10));

var productGroups = jobs.Stage("productGroups").HandledBy<ProductGroupsService>()
    .DependsOnInstance(shops)
    .RunPeriodically(TimeSpan.FromMinutes(30));

var products = jobs.Stage("products").HandledBy<ProductsService>()
    .DependsOn(productGroups)
    .RunPeriodically(TimeSpan.FromMinutes(5));

var documents = jobs.Stage("documents").HandledBy<DocumentsService>()
    .DependsOn(products).DependsOn(employees)
    .RunPeriodically(TimeSpan.FromMinutes(2));
```

После трёх вызовов `ctx.AddKeyAsync("uuid-N")` в `ShopsService` имеем:

| Сущность | Кол-во | Примеры |
|---|---|---|
| `Stage`-объекты в `StageRegistry` | 5 | `shops`, `employees`, `productGroups`, `products`, `documents` |
| `Instance`-объекты в `InstanceManager` | 14 | `shops[]`, `employees[]`, `productGroups[shops=u1..u3]` (×3), `products[shops=u1..u3]` (×3), `documents[shops=u1..u3]` (×3) — итого 1+1+3+3+3 = 11; до первого успеха `employees`/`products`/`documents` будут только частично созданы. После полной волны bootstrap — 11. Точно 14 будет в более широких сценариях с partial keys. |
| Jobs (итерации в час, грубо) | сотни | timer каждой стадии тикает по своему интервалу |

Каждый `Instance` имеет один Timer (`InstanceTimer`). При его тике публикуется `TimerTickedEvent(Instance)`. EventLoop принимает решение, запускать ли итерацию (=Job).

## Чек-лист для понимания при чтении кода SDK

- Видишь `StageDescriptor` — это **декларация** (статика).
- Видишь `Instance` или поле `Instance` в event-record — это **runtime-экземпляр** (long-lived).
- Видишь `IJobService.ExecuteAsync` или `JobContext` — это **итерация** (одна материализация работы).
- `InstanceManager` — owner всех `Instance`-объектов.
- `StageRegistry` — owner всех `StageDescriptor`-объектов.
- `InstancesOverview` / `InstanceInfo` — снапшот всех `Instance` для диагностики.
