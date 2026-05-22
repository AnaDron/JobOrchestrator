# Архитектура

Job Orchestrator — SDK периодических задач с зависимостями. Описание ниже — текущее состояние реализации, не roadmap. Для введения в концепты Stage / Instance / Job см. [concepts.md](concepts.md).

## Ключевые принципы

1. **Single-threaded event loop.** Один consumer-поток в `Channel<OrchestratorEvent>` обрабатывает все события (TimerTicked, ManualTriggerRequested, KeyAdded, KeyRemoved, StageCompleted, StageFailed). Все state-transitions инстансов происходят на этом потоке без блокировок.
2. **Multi-threaded publishers.** Внешние API (`IJobOrchestrator`), `DueScanner`, fire-and-forget runner-ы публикуют события в Channel из любых потоков. Channel-Writer thread-safe by design.
3. **Lock-free read API.** `IJobOrchestrator.GetOverview()` — синхронный snapshot через atomic `Volatile.Read` всех мутирующихся полей `Instance` (UtcTicks-encoded). Не идёт через event loop, не блокирует и не аллоцирует CTS.
4. **DI scope per iteration.** На каждую итерацию `IJobService.ExecuteAsync` создаётся свежий `IServiceScope`. Scoped-сервисы (например, `DbContext`) уникальны для одной итерации — достаточно для большинства транзакционных требований.
5. **In-memory only, single orchestrator per process.** Граф стадий, состояние инстансов, keyspace, retry-счётчики — RAM одного процесса. Несколько инстансов приложения, пишущих в один «логический» граф без внешней координации (lease/leader), **не поддерживаются** — см. `TODO.md` (distributed execution). Рестарт = bootstrap с нуля. Состояние БЛ persists через `IJobState` (нейтральный key-value bag), backend через `IJobStateStore` (по умолчанию `InMemoryJobStateStore`; внешние backends — отдельные пакеты).

## Поток событий

```
              Producers (any thread)                Consumer (single thread)
              ─────────────────────                 ───────────────────────
   orchestrator.Root["s"][keys].RunAsync ──────┐
   orchestrator.Root["s"].RegisterKey ─────────┤
                                          ├──► Channel<OrchestratorEvent> ──► EventLoop.RunAsync
   ctx.AddKeyAsync  (из IJobService) ─────┤        (bounded 10_000)            ├─ HandleTimerTick
   ctx.RemoveKeyAsync ────────────────────┤                                    ├─ HandleManualTrigger
   DueScanner.RunAsync ───────────────────┤                                    ├─ HandleKeyAdded
   StageRunner finalize ──────────────────┘                                    ├─ HandleKeyRemoved
                                                                               ├─ HandleStageCompleted
                                                                               └─ HandleStageFailed

   IJobOrchestrator.GetOverview() ──► (lock-free) InstanceManager.Snapshot()  — НЕ идёт в event loop
```

Все публикации идут через `ChannelWriterExtensions` (`PublishAsync` / sync-`Publish`):

- **Backpressure:** bounded-channel (10 000). Общий fast-path `TryWrite`; slow-path — `WriteAsync`. `PublishAsync` — `ctx.AddKeyAsync`/`RemoveKeyAsync`, `RunAsync` (async ожидание слота). Sync-`Publish` — completion runner-а, `TimerTicked`, `RegisterKey`/`UnregisterKey` (блокирует caller); не с UI/HTTP request thread.
- **Shutdown:** `ChannelClosedException` при `Publish` поглощается; `StageRunner` снимает `Running`, если completion не опубликован; события, уже лежащие в channel, дочищаются в `EventLoop.DrainPendingRequests` (`EndRunning` + сигнал waiters).

## DueScanner

Единственный pull-based планировщик вместо per-инстансового `Timer`. Алгоритм:

1. Снимок `InstanceManager.All` (lock-free, `ConcurrentDictionary.Values`).
2. Для каждого Idle-инстанса с `NextAutoUtc ≤ now` — публикует `TimerTickedEvent`.
3. Находит минимум `NextAutoUtc` среди оставшихся, спит до этого момента (или `MaxSleep` = 30 c для idle-режима).
4. Сон прерывается через wake-up CTS — `Wake()` вызывается event loop'ом при изменении расписания (создание инстанса, schedule-next-tick после Completed/Failed, KeyAdded/Removed-каскады).

Преимущества vs per-инстанс `Timer`: один аллокированный объект вместо N, линейный scan кеш-friendly при типичных N=10–100. Trade-off отражён в TODO: при N > 10k имеет смысл sorted-by-deadline collection.

## Каскадное удаление инстансов

`ctx.RemoveKeyAsync(K)` или `orchestrator.Root["stage"].UnregisterKey(K)` инициирует двухэтапный cleanup:

**Этап 1 (синхронно в `HandleKeyRemovedAsync`):**
- Транзитивное замыкание стадий через pre-computed `StageDescriptor.AffectedByKeyRemoval` (O(1)).
- Сортировка по pre-computed `StageRegistry.CancellationRank` (листья имеют меньший ранг, отменяются первыми).
- Для каждого аффектированного инстанса: удалить из `InstanceManager` (NextAutoUtc больше не виден DueScanner-у), если Running — `RunCts.Cancel()` + добавить в `_terminating` set. `RemoveScopeAsync` НЕ вызывается.

**Этап 2 (асинхронно в `HandleStageCompletedAsync`/`HandleStageFailedAsync`):**
- Когда runner cancelled-итерации публикует `StageCompletedEvent` или `StageFailedEvent`, handler видит инстанс в `_terminating` set, идёт в `FinalizeTerminatingAsync` — вызывает `RemoveScopeAsync`. Обычный путь (обновление LastSuccess / ConsecutiveFailures / перепланирование timer) пропускается.

Event loop за время отмены и завершения runner-а **не блокируется** — продолжает обрабатывать другие события. Cleanup `RemoveScopeAsync` гарантированно happens-after физического завершения итерации, поэтому не конкурирует с её writes в `IJobState`.

## Фильтрация эмиссий от cancelling-инстансов

События `KeyAddedEvent`/`KeyRemovedEvent` несут поле `Source: Instance?`:
- `null` — событие от внешнего вызова `RegisterKey`/`UnregisterKey`.
- `instance` — событие от `ctx.AddKeyAsync`/`ctx.RemoveKeyAsync` внутри `IJobService.ExecuteAsync`.

`HandleKeyAdded`/`HandleKeyRemoved` проверяют: если `Source ∈ _terminating`, событие игнорируется (cancelling-инстанс не может породить новые child-инстансы или удалить ключи в свои финальные миллисекунды).

## Создание инстансов: backtracking-merge

`InstanceCreator.EvaluateAndCreate(stage)` строит candidate-`DependencyKeys` инкрементально:

```
Recurse(stage, dimensions, dimIdx, currentDict):
    if dimIdx == dimensions.Count:
        if не существует && все deps разрешены:
            создать инстанс с нормализованным порядком ключей
        return

    for candidate in dimensions[dimIdx]:
        rollback = []
        for kv in candidate:
            if currentDict has kv.Key with same value: skip
            if currentDict has kv.Key with DIFFERENT value: incompatible, break
            else: currentDict.Add(kv), rollback.Add(kv.Key)

        if not incompatible:
            Recurse(stage, dimensions, dimIdx+1, currentDict)

        # backtrack
        for k in rollback: currentDict.Remove(k)
```

Сложность: O(совместимые_пути × N) вместо O(полный_cartesian). На типовом Эвотор-сценарии с общим ключом `shops` через 5 зависимостей: 10⁴ итераций вместо 10¹⁰.

Порядок ключей в финальном `DependencyKeys` нормализуется по `stage.Dependencies` (= Fluent API order) — обеспечивает стабильный `FullyQualifiedName` независимо от того, в каком порядке прилетали разрешающие события.

## Принятие триггеров

```
TryAccept(instance, source, now):
    if instance.State == Terminating: return Terminating
    if instance.State == Running: return AlreadyRunning

    if source == Auto:
        if ConsecutiveFailures > 0 && now - LastAttempt < RetryDelay(ConsecutiveFailures):
            return WaitingRetry
        policy OK → BeginIteration

    # source == Manual: retry-delay НЕ блокирует, debounce уважается
    if LastAttempt != null && now - LastAttempt < Debounce:
        return Debounced
    policy OK → BeginIteration

BeginIteration (после принятия политики):
    if GlobalConcurrencyLimit or ConcurrencyLimit exhausted: return ConcurrencyDeferred
    TryBeginRunning → Started (runner на ThreadPool)
```

- `Started` — только после успешного `TryBeginRunning` (event-loop конструирует `IIterationHandle` и резолвит им `TCS` ответа `RunAsync`).
- `ConcurrencyDeferred` — лимит параллелизма (глобальный или per-stage), не путать с `WaitingRetry` (retry-delay после неуспеха).
- `Retry-delay` — защита downstream от Auto-spam после неуспеха. Manual игнорирует — пользователь явно просит.
- `Debounce` — анти-spam-click для Manual. От `LastAttempt`, не от `LastSuccess`, поэтому работает и после успеха, и после неуспеха.
- `Auto` debounce не проверяет — scheduled tick через interval сам по себе соблюдает темп.

## Жизненный цикл оркестратора

`OrchestratorLifecycle` владеет тремя состояниями: **Running** (нормальная работа), **Faulted** (краш event loop), **Stopped** (graceful shutdown).

- `MarkFaulted` — выставляет `IsFaulted = true`, cancel-ит `WorkersCancellationToken` (running iterations завершаются досрочно), `channel.Writer.Complete()`. Вызывается из `JobOrchestratorHostedService.ExecuteAsync` при необработанном исключении всего loop, либо из `EventLoop` после `JobOrchestratorHostOptions.HandlerCrashFaultThreshold` подряд сбоев handler'ов.
- `CloseChannel` — `channel.Writer.Complete()` без cancel workers. Вызывается при graceful shutdown.
- `CancelRunningWorkers` — cancel `WorkersCancellationToken` **без** fault; после `CloseChannel` + `ShutdownIterationTimeout` (если задан в `JobOrchestratorHostOptions`).

**Сбой отдельного handler'а** (исключение в `HandleEventAsync`): `ManualTriggerRequestedEvent` → `Tcs.TrySetException(ex)`. Если `FaultOnStateMutatingHandlerCrash` (default **true**) и событие мутирует граф (AddKey/RemoveKey, completion, tick) — **сразу** `MarkFaulted`. Иначе loop продолжается до `HandlerCrashFaultThreshold` подряд (default 3).

**Итерации на ThreadPool:** `RunIterationSafeAsync` ловит необработанные исключения runner'а (вне `StageFailed`) и логирует — unobserved task fault не теряется.

**ConcurrencyDeferred:** `NextAutoUtc = now + 1s + jitter(0..ConcurrencyDeferJitterMaxMilliseconds)` (default 500 ms) — снижает thundering herd на глобальном/per-stage лимите.

После выхода из `EventLoop.RunAsync` любой причиной (cancel/exception/channel-closed) `finally`-блок:
1. `DrainPendingRequests` — `ManualTrigger` → `TrySetException` (shutdown); `StageCompleted`/`Failed` → **метрики** + waiters + `EndRunning` (без first-success cascade); `TimerTicked` → `ReleasePendingTick`; KeyAdded/Removed — debug-log и discard.
2. `FinalizeAllTerminatingAsync` — для застрявших terminating-инстансов вызывается `RemoveScopeAsync` (best-effort, исключения логируются).

После Faulted внешний API (`IJobOrchestrator`):
- `orchestrator.Root["stage"][keys].RunAsync()` бросает `IterationRejectedException(Reason=Faulted)`.
- `orchestrator.Root["stage"].RegisterKey` / `UnregisterKey` бросают `InvalidOperationException`.
- `GetOverview` доступен (для post-mortem snapshot'а — что было в InstanceManager на момент краша).

## Restart-behavior

SDK хранит весь runtime-state in-memory: `InstanceManager`, `KeyspaceRegistry`, `JobMetrics`. При перезапуске процесса всё сбрасывается:

- `LastSuccess` всех инстансов → `null` (никто не имеет «истории успеха»).
- `KeyspaceRegistry` пуст.
- `InstanceManager` содержит только bootstrap-keyless-стадии (через `BootstrapInitialInstances`).
- Ключевые/dependent-стадии не существуют, пока их родители не наберут первый success.

**Что переживает рестарт:** только данные в `IJobStateStore` (per-instance JSON-bag) — в дефолтной реализации `InMemoryJobStateStore` они тоже сбрасываются, но кастомные backend-ы (SQL, Redis) могут сохранять.

**Следствия для БЛ:**
- `JobContext.LastSuccessAt == null` на первой итерации после рестарта → БЛ должна различать full-sync vs delta-sync через эту метку.
- `RegisterKey` для bootstrap keyspace из БД — типовой production-pattern (см. Sample).
- Каскад «first success» прорастает заново через всю цепочку DependsOn-зависимостей.

## InstanceLifecycleState

Три состояния:
- **Idle** — инстанс существует, ждёт следующего тика или истечения retry-delay.
- **Running** — итерация выполняется. DueScanner и Manual-триггеры пропускают (return `AlreadyRunning`).
- **Terminating** — cascade-removal в процессе. RunCts отменён (для бывших Running). Инстанс остаётся в `InstanceManager` до finalize. Триггеры и DueScanner пропускают; Manual возвращает `TriggerResult.Terminating`.

**State-transition diagram:**

```
                ┌───────────────┐
   bootstrap →  │     Idle      │ ←──── StageCompleted/Failed
                └───┬───────┬───┘
                    │       │
        Begin       │       │ cascade-removal
        Iteration   │       │ (Idle → finalize sync)
                    ↓       ↓
                ┌───────────────┐       ┌──────────────────┐
                │    Running    │ ────→ │   Terminating    │ ──→ FinalizeTerminating
                └───────────────┘  c-c  └──────────────────┘     (Remove + RemoveScope)
                                                ↑
                                        cascade ↑
                                  (Running → wait StageCompleted/Failed)
```

Переход в Terminating атомарен (`Volatile.Write` int-поля); читается лениво атомарно (`Volatile.Read`).

## Структурное логирование

`StageRunner.RunIterationAsync` оборачивает каждую итерацию в `logger.BeginScope` со следующими полями:
- `CorrelationId` (GUID per итерация).
- `FullyQualifiedName` (полный канонический идентификатор инстанса).
- `StageName`.
- `{depStageName}Key` — отдельное поле per компонент `DependencyKeys` (например, `shopsKey = "ab12-c34d"`). Суффикс `Key` исключает коллизии с Serilog-предопределёнными именами и подчёркивает «компонент композитного ключа».

Это даёт точечные Seq-фильтры типа `@StageName = 'documents' and @shopsKey = 'ab12-c34d'`, в отличие от substring-match по `FullyQualifiedName`.

## DependencyKey: encoding и FullyQualifiedName

`DependencyKey.Encode` — канонический encoding для словарного ключа `InstanceManager`. Компоненты отсортированы по имени (sort by name), специальные символы `|`, `=`, `\` экранируются `\` — исключает collision-возможность вида `{a: "1|b=2"}` vs `{a: "1", b: "2"}`.

`DependencyKey.FormatFullyQualifiedName` — человеко-читаемый идентификатор: `"stage[dep1=v1,dep2=v2,...]"` без пробелов после запятых, компоненты в порядке `stage.Dependencies` (Fluent API order). Для безключевой стадии — `"stage[]"`.

## InstanceManager

Owner всех `Instance`-объектов. Writes — через event-loop consumer thread (single writer); reads — из любого потока (`DueScanner`, `GetOverview()`), thread-safe by `ConcurrentDictionary`:
- Primary index: `ConcurrentDictionary<(StageName, EncodedKey), Instance>`.
- Secondary index: `ConcurrentDictionary<StageName, ConcurrentDictionary<Instance, byte>>` — O(1) для `InstancesOf(stageName)` (используется в `InstanceCreator.ComputeDimension` и `DependencyResolver.FindPairedInstance`).
- `Snapshot()` — eventually-consistent копия в `InstancesOverview` через atomic-reads `Instance`-полей. Lock-free.

## Instance: snapshot-consistent метрики

Мутирующее состояние инстанса хранится как immutable-record `JobMetrics` (`InstanceExecutionStats` + `InstanceSchedule`), заменяемый атомарно через `Interlocked.Exchange`. Read-side получает согласованную пару Stats/Schedule из одной «эпохи» writer'а (через `instance.Metrics` → один `Volatile.Read`).

**Без facade-полей:** `instance.LastSuccess` и аналоги намеренно отсутствуют — call-sites обязаны делать `instance.Metrics` явно. Это сразу показывает места, где Metrics читается несколько раз → optimization-hotspots (один snapshot + локальная переменная).

`State` (Idle/Running/Terminating) — отдельный atomic int (меняется независимо от метрик). `RunCts` — `volatile`-reference для memory-ordering между runner-thread и event-loop-thread. `pendingTick` — CAS-флаг для idempotency DueScanner-а.

## InstanceIdentity

Иммутабельный композитный идентификатор `(StageDescriptor, ImmutableDictionary<string,string>)`. Pre-computed:
- `EncodedKey` — canonical encoding (sort by name, escaped `|=\`-символы) для словарных lookup'ов.
- `FullyQualifiedName` — human-readable для логов.
- `StateScope` — `"{StageName}:{EncodedKey}"` для `IJobStateStore`.

Equality по `(StageName, EncodedKey)`. Используется как primary-ключ в `InstanceManager`. Один Identity-объект живёт всю lifetime инстанса.

## Pre-computed StageRegistry

В конструкторе `StageRegistry` один раз вычисляются (графовые поля живут на `StageDescriptor`):
- `StageDescriptor.DependentsWhole` / `StageDescriptor.DependentsInstance` — обратные индексы зависимостей, O(1) lookup.
- `StageDescriptor.AffectedByKeyRemoval` — BFS-замыкание per стадия, кэшируется в словарь.
- `CancellationRank(name)` — глубина «вниз по графу» (лист = 0, корень = max), используется для сортировки cascade-removal без повторного topo-sort-а каждый раз.
- `ExpectedKeyNames(name)` — транзитивно унаследованные имена ключей через цепочку `DependsOn`/`DependsOnInstance`, используется в `ValidateKeys` (handle-indexer).

## ConcurrencyLimits и GlobalIterationLimiter

**Per-stage** (`ConcurrencyLimits`): `SemaphoreSlim` на стадию с `WithConcurrencyLimit(int)`. **Глобальный** (`GlobalIterationLimiter`): `jobs.Defaults.GlobalConcurrencyLimit = N` — суммарный cap итераций по всем стадиям.

Acquire в `EventLoop.BeginIteration` (сначала global, затем per-stage) до `TryBeginRunning`. При исчерпании — `TriggerResult.ConcurrencyDeferred`, re-schedule через 1 sec. Release — в `StageRunner.RunIterationAsync` finally.

## Out of scope

Список отложенных features в [TODO.md](../TODO.md).
