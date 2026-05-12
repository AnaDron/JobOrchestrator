namespace JobOrchestrator.Internal;

/// <summary>Внутренние события event loop'а оркестратора.</summary>
internal abstract record OrchestratorEvent;

/// <summary>Auto-тик для конкретного инстанса.</summary>
internal sealed record TimerTickedEvent(StageInstance Instance) : OrchestratorEvent;

/// <summary>Запрос ручного триггера снаружи (через <see cref="IJobOrchestrator.TriggerAsync"/>).</summary>
internal sealed record ManualTriggerRequestedEvent(
	string StageName,
	IReadOnlyDictionary<string, string> DependencyKeys,
	TaskCompletionSource<TriggerResult> Tcs
) : OrchestratorEvent;

/// <summary>Регистрация ключа в keyspace стадии. Источник: <c>ctx.AddKey</c> или <c>IJobOrchestrator.RegisterKey</c>.</summary>
internal sealed record KeyAddedEvent(string StageName, string Key) : OrchestratorEvent;

/// <summary>Удаление ключа из keyspace стадии. Источник: <c>ctx.RemoveKey</c> или <c>IJobOrchestrator.UnregisterKey</c>.</summary>
internal sealed record KeyRemovedEvent(string StageName, string Key) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась успешно (без исключения).</summary>
internal sealed record StageCompletedEvent(StageInstance Instance) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась неуспехом (исключение или watchdog cancel).</summary>
internal sealed record StageFailedEvent(StageInstance Instance, Exception Exception) : OrchestratorEvent;
