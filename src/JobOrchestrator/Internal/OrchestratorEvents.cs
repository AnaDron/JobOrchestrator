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
/// Регистрация ключа в keyspace стадии. <paramref name="Source"/> — инстанс-эмитент
/// (для <c>ctx.AddKey</c>) или <c>null</c> (для <see cref="IJobOrchestrator.RegisterKey"/>).
/// Используется для фильтрации событий от terminating-инстансов.
/// </summary>
internal sealed record KeyAddedEvent(string StageName, string Key, StageInstance? Source = null) : OrchestratorEvent;

/// <summary>
/// Удаление ключа из keyspace стадии. <paramref name="Source"/> — инстанс-эмитент (для <c>ctx.RemoveKey</c>)
/// или <c>null</c> (для <see cref="IJobOrchestrator.UnregisterKey"/>).
/// </summary>
internal sealed record KeyRemovedEvent(string StageName, string Key, StageInstance? Source = null) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась успешно (без исключения). <paramref name="At"/> зафиксировано в runner-thread.</summary>
internal sealed record StageCompletedEvent(StageInstance Instance, DateTimeOffset At) : OrchestratorEvent;

/// <summary>Итерация инстанса завершилась неуспехом (исключение или watchdog cancel).</summary>
internal sealed record StageFailedEvent(StageInstance Instance, Exception Exception, DateTimeOffset At) : OrchestratorEvent;
