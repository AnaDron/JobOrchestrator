# Архитектура

Job Orchestrator — SDK периодических задач с зависимостями. Описание ниже — текущее состояние реализации, не roadmap. Для введения в концепты Stage / StageInstance / Job см. [concepts.md](concepts.md).

## Ключевые принципы

1. **Single-threaded event loop.** Один consumer-поток в `Channel<OrchestratorEvent>` обрабатывает все события (TimerTicked, ManualTriggerRequested, KeyAdded, KeyRemoved, StageCompleted, StageFailed, OverviewRequested). Все state-transitions инстансов происходят на этом потоке без блокировок и без CAS-операций.
2. **Multi-threaded publishers.** Внешние API (`IJobOrchestrator`), per-instance таймеры, fire-and-forget runner-ы публикуют события в Channel из любых потоков. Channel-Writer thread-safe by design.
3. **DI scope per iteration.** На каждую итерацию `IJobService.ExecuteAsync` создаётся свежий `IServiceScope`. Scoped-сервисы (например, `DbContext`) уникальны для одной итерации — достаточно для большинства транзакционных требований.
4. **In-memory only.** Граф стадий, состояние инстансов, keyspace, retry-счётчики — RAM. Рестарт = bootstrap с нуля. Состояние БЛ persists через `IJobState` (нейтральный key-value bag), backend через `IJobStateStore` (по умолчанию `InMemoryJobStateStore`; внешние backends — отдельные пакеты).

## Поток событий

```
              Producers (any thread)                Consumer (single thread)
              ─────────────────────                 ───────────────────────
   IJobOrchestrator.TriggerAsync ──┐
   IJobOrchestrator.RegisterKey ───┤
   IJobOrchestrator.GetOverview ───┤
                                   ├──► Channel<OrchestratorEvent> ──► EventLoop.RunAsync
   ctx.AddKey  (из IJobService) ───┤        (bounded 10_000)            ├─ HandleKeyAdded
   ctx.RemoveKey ──────────────────┤                                    ├─ HandleKeyRemoved
   InstanceTimer callback ─────────┤                                    ├─ HandleStageCompleted
   StageRunner finalize ───────────┘                                    ├─ HandleStageFailed
                                                                        ├─ HandleTimerTick
                                                                        ├─ HandleManualTrigger
                                                                        └─ HandleOverviewRequested
```

Все публикации идут через `ChannelWriter<T>.Publish` (см. `ChannelWriterExtensions`) — fast-path TryWrite, fallback на sync-WriteAsync при заполнении bounded-channel. `ChannelClosedException` поглощается (после shutdown/crash).

## Каскадное удаление инстансов

`ctx.RemoveKey(K)` или `IJobOrchestrator.UnregisterKey(stage, K)` инициирует двухэтапный cleanup:

**Этап 1 (синхронно в `HandleKeyRemovedAsync`):**
- Транзитивное замыкание стадий, чьи DependencyKeys могут содержать `{stage: K}`.
- Топологически обратный порядок (листья перед корнями).
- Для каждого аффектированного инстанса: удалить из `InstanceManager`, dispose `InstanceTimer`, если Running — `RunCts.Cancel()` + добавить в `_terminating` set. `RemoveScopeAsync` НЕ вызывается.

**Этап 2 (асинхронно в `HandleStageCompletedAsync`/`HandleStageFailedAsync`):**
- Когда runner cancelled-итерации публикует `StageCompletedEvent` или `StageFailedEvent`, handler видит инстанс в `_terminating` set, идёт в `FinalizeTerminatingAsync` — вызывает `RemoveScopeAsync`. Обычный путь (обновление LastSuccess / ConsecutiveFailures / перепланирование timer) пропускается.

Event loop за время отмены и завершения runner-а **не блокируется** — продолжает обрабатывать другие события. Cleanup `RemoveScopeAsync` гарантированно happens-after физического завершения итерации, поэтому не конкурирует с её writes в `IJobState`.

## Фильтрация эмиссий от cancelling-инстансов

События `KeyAddedEvent`/`KeyRemovedEvent` несут поле `Source: StageInstance?`:
- `null` — событие от внешнего вызова `RegisterKey`/`UnregisterKey`.
- `instance` — событие от `ctx.AddKey`/`ctx.RemoveKey` внутри `IJobService.ExecuteAsync`.

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
TryAcceptTrigger(instance, source, now):
    if instance.State == Running: return AlreadyRunning

    if source == Auto:
        if ConsecutiveFailures > 0 && now - LastAttempt < RetryDelay(ConsecutiveFailures):
            return WaitingRetry
        return Started

    # source == Manual: retry-delay НЕ блокирует, debounce уважается
    if LastAttempt != null && now - LastAttempt < Debounce:
        return Debounced
    return Started
```

- `Retry-delay` — защита downstream от Auto-spam после неуспеха. Manual игнорирует — пользователь явно просит.
- `Debounce` — анти-spam-click для Manual. От `LastAttempt`, не от `LastSuccess`, поэтому работает и после успеха, и после неуспеха.
- `Auto` debounce не проверяет — scheduled tick через interval сам по себе соблюдает темп.

## Жизненный цикл оркестратора

`OrchestratorLifecycle` владеет тремя состояниями: **Running** (нормальная работа), **Faulted** (краш event loop), **Stopped** (graceful shutdown).

- `MarkFaulted` — выставляет `IsFaulted = true`, cancel-ит `WorkersCancellationToken` (running iterations завершаются досрочно), `channel.Writer.Complete()`. Вызывается из `JobOrchestratorHostedService.ExecuteAsync` в `catch (Exception)`.
- `CloseChannel` — `channel.Writer.Complete()` без cancel workers. Вызывается при graceful shutdown (workers уже cancel через `BackgroundService.stoppingToken`).

После выхода из `EventLoop.RunAsync` любой причиной (cancel/exception/channel-closed) `finally`-блок:
1. `DrainPendingRequests` — оставшиеся `OverviewRequestedEvent` получают `TrySetException`, `ManualTriggerRequestedEvent` — `TrySetResult(Faulted)`. Иначе caller-ы зависли бы на `await tcs.Task`.
2. `FinalizeAllTerminatingAsync` — для застрявших terminating-инстансов вызывается `RemoveScopeAsync` (best-effort, исключения логируются).

После Faulted внешний API (`IJobOrchestrator`):
- `TriggerAsync` возвращает `TriggerResult.Faulted`.
- `RegisterKey` / `UnregisterKey` бросают `InvalidOperationException`.
- `GetOverviewAsync` бросает `InvalidOperationException`.

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

Owner всех `StageInstance`-объектов. Operations через event-loop consumer thread (single-threaded), без блокировок:
- Primary index: `Dictionary<(StageName, EncodedKey), StageInstance>`.
- Secondary index: `Dictionary<StageName, HashSet<StageInstance>>` — O(1) для `InstancesOf(stageName)` (часто используется в `InstanceCreator.ComputeDimension` и `DependencyResolver.FindPairedInstance`).
- `ToOverview()` — snapshot всех инстансов в `InstancesOverview`. Вызывается из event-loop при `OverviewRequestedEvent`.

## Out of scope

Список отложенных features в [TODO.md](../TODO.md):
- `DependsOnAll(X)` — universal-блокировка (все инстансы X успешны).
- ActivitySource / OpenTelemetry-инструментация стадий.
- REST-обёртка `IJobOrchestrator`.
- Внешние backend-ы `IJobStateStore` (SQL/Redis/file).
- Условные зависимости (предикат в `DependsOn*`).
- Distributed execution (multi-instance с внешним lease).
- Concurrency limits.
