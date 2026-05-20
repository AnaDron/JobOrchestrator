namespace JobOrchestrator.Internal;

/// <summary>Внутренние события event loop'а оркестратора.</summary>
internal abstract record OrchestratorEvent;

/// <summary>Auto-тик для конкретного инстанса. Публикуется <see cref="DueScanner"/> при наступлении <c>NextAutoUtc</c>.</summary>
internal sealed record TimerTickedEvent(Instance Instance) : OrchestratorEvent;

/// <summary>
/// Запрос ручного триггера снаружи (через <see cref="IInstanceHandle.RunAsync"/>).
/// <see cref="Identity"/> уже валидирован в <c>JobOrchestratorRuntime</c> (stage существует в графе,
/// keys соответствуют <c>ExpectedKeyNames</c>) и pre-computed encoded-key — event-loop делает прямой
/// <c>instances.Find(Identity)</c>, без повторного encode на каждый trigger.
/// <para>
/// <see cref="Tcs"/> — единый канал ответа event-loop'а caller'у: на Started event-loop конструирует
/// <see cref="IterationHandle"/>, привязывает его outcome-TCS к <see cref="Instance"/> и резолвит
/// этим handle. На любой reject (Debounced/NotFound/AlreadyRunning/...) event-loop отстреливает
/// <see cref="IterationRejectedException"/> через <c>TrySetException</c>.
/// </para>
/// </summary>
internal sealed record ManualTriggerRequestedEvent(
	InstanceIdentity Identity,
	TaskCompletionSource<IIterationHandle> Tcs
) : OrchestratorEvent;

/// <summary>
/// Регистрация ключа в keyspace инстанса-эмитера. <paramref name="Source"/> идентифицирует bucket —
/// комбинация <c>Source.Stage.Name + Source.DependencyKeys</c>. Для внешнего
/// <see cref="IStageHandle.RegisterKey"/> Source резолвится в keyless-инстанс стадии (это работает
/// только для стадий без <c>DependsOnInstance</c>).
/// </summary>
internal sealed record KeyAddedEvent(Instance Source, string Key) : OrchestratorEvent;

/// <summary>
/// Удаление ключа из keyspace инстанса-эмитера. <paramref name="Source"/> идентифицирует bucket
/// аналогично <see cref="KeyAddedEvent"/>.
/// </summary>
internal sealed record KeyRemovedEvent(Instance Source, string Key) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась успешно (без исключения). <paramref name="At"/> зафиксировано в runner-thread.</summary>
internal sealed record StageCompletedEvent(Instance Instance, DateTimeOffset At) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась неуспехом (исключение или watchdog cancel).</summary>
internal sealed record StageFailedEvent(Instance Instance, Exception Exception, DateTimeOffset At) : OrchestratorEvent;
