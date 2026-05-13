namespace JobOrchestrator.Internal;

/// <summary>Внутренние события event loop'а оркестратора.</summary>
internal abstract record OrchestratorEvent;

/// <summary>Auto-тик для конкретного инстанса. Публикуется <see cref="DueScanner"/> при наступлении <c>NextAutoUtc</c>.</summary>
internal sealed record TimerTickedEvent(StageInstance Instance) : OrchestratorEvent;

/// <summary>Запрос ручного триггера снаружи (через <see cref="IJobOrchestrator.TriggerAsync"/>).</summary>
internal sealed record ManualTriggerRequestedEvent(
	string StageName,
	IReadOnlyDictionary<string, string> DependencyKeys,
	TaskCompletionSource<TriggerResult> Tcs
) : OrchestratorEvent;

/// <summary>
/// Регистрация ключа в keyspace инстанса-эмитера. <paramref name="Source"/> идентифицирует bucket —
/// комбинация <c>Source.Stage.Name + Source.DependencyKeys</c>. Для внешнего
/// <see cref="IJobOrchestrator.RegisterKey"/> Source резолвится в keyless-инстанс стадии (это работает
/// только для стадий без <c>DependsOnInstance</c>).
/// </summary>
internal sealed record KeyAddedEvent(StageInstance Source, string Key) : OrchestratorEvent;

/// <summary>
/// Удаление ключа из keyspace инстанса-эмитера. <paramref name="Source"/> идентифицирует bucket
/// аналогично <see cref="KeyAddedEvent"/>.
/// </summary>
internal sealed record KeyRemovedEvent(StageInstance Source, string Key) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась успешно (без исключения). <paramref name="At"/> зафиксировано в runner-thread.</summary>
internal sealed record StageCompletedEvent(StageInstance Instance, DateTimeOffset At) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась неуспехом (исключение или watchdog cancel).</summary>
internal sealed record StageFailedEvent(StageInstance Instance, Exception Exception, DateTimeOffset At) : OrchestratorEvent;
