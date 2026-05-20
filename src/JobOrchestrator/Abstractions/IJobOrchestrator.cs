namespace JobOrchestrator.Abstractions;

/// <summary>
/// Точка управления оркестратором снаружи: ручные триггеры, наполнение keyspace для bootstrap, диагностика.
/// Резолвится из DI как singleton.
/// <para>
/// <b>Handle-API:</b> <c>orchestrator["stageName"][InstanceKey.None | (key, value), ...].Operation()</c>
/// — структурированный доступ через <see cref="IStageHandle"/> → <see cref="IInstanceHandle"/>.
/// </para>
/// <para>
/// <b>Enumeration:</b> <see cref="IJobOrchestrator"/> сам по себе является <see cref="IEnumerable{IStageHandle}"/>:
/// <c>foreach (var stage in orchestrator) { ... }</c> — итерация всех зарегистрированных стадий.
/// </para>
/// <para>
/// <b>Кэширование handles в hot-path:</b> indexer-вызовы создают per-call аллокации (InstanceHandle + Identity).
/// Для polling-сценариев (UI-обновления, метрики) — кэшируйте handle локально:
/// <code>
/// var h = orchestrator["pg"][("shops", "u1")];   // один раз
/// while (running) {
///     var state = h.State;                        // re-uses cached identity
///     await Task.Delay(...);
/// }
/// </code>
/// </para>
/// </summary>
public interface IJobOrchestrator : IEnumerable<IStageHandle> {
	/// <summary>
	/// <c>true</c>, если event loop крашнулся (Faulted). После Faulted-состояния все методы либо возвращают
	/// <see cref="TriggerResult.Faulted"/>, либо бросают <see cref="InvalidOperationException"/>.
	/// </summary>
	bool IsFaulted { get; }

	/// <summary>
	/// Root handle-API: <c>orchestrator["stageName"]</c> → <see cref="IStageHandle"/>.
	/// O(1) hash-lookup в pre-populated кеше; нулевая allocation.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадия не зарегистрирована в графе.</exception>
	IStageHandle this[string stageName] { get; }

	/// <summary>
	/// Синхронный снимок состояния всех существующих инстансов всех стадий. Lock-free через atomic-reads
	/// (<see cref="System.Threading.Volatile.Read{T}(ref T)"/>) — eventually consistent между полями одного инстанса,
	/// но всегда без race-conditions на уровне snapshot collection.
	/// </summary>
	InstancesOverview GetOverview();

	/// <summary>
	/// Domain-проекция: <c>orchestrator.WithDomain("evotor")["shops"][keys].RunAsync()</c>.
	/// Возвращаемый <see cref="IDomainScopedJobOrchestrator"/> кэшируется per-domain — multiple вызовы
	/// с тем же <paramref name="domain"/> дают тот же объект (без per-call аллокаций).
	/// </summary>
	IDomainScopedJobOrchestrator WithDomain(string domain);
}
