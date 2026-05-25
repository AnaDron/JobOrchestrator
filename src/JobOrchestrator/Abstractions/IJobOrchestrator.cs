namespace JobOrchestrator.Abstractions;

/// <summary>
/// Точка управления оркестратором снаружи: ручные триггеры, наполнение keyspace для bootstrap, диагностика.
/// Резолвится из DI как singleton.
/// <para>
/// <b>Handle-API:</b> <c>orchestrator["domain"]["stage"][keys].Operation()</c> — структурированный доступ
/// через <see cref="IDomainHandle"/> → <see cref="IStageHandle"/> → <see cref="IInstanceHandle"/>. Для
/// бездоменных стадий — <c>orchestrator.Root["stage"][keys]</c>.
/// </para>
/// <para>
/// <b>Enumeration:</b> <see cref="IJobOrchestrator"/> — это <see cref="IReadOnlyCollection{IDomainHandle}"/>:
/// <c>foreach (var domain in orchestrator) { ... }</c> перечисляет все домены, root-домен присутствует первым
/// с <c>Name == <see cref="DomainName.Root"/></c>.
/// </para>
/// <para>
/// <b>Кэширование handles в hot-path:</b> indexer-вызовы создают per-call аллокации (InstanceHandle + Identity).
/// Для polling-сценариев (UI-обновления, метрики) — кэшируйте handle локально:
/// <code>
/// var h = orchestrator["catalog"]["shops"][("region", "EU")];  // один раз
/// while (running) {
///     var state = h.State;
///     await Task.Delay(...);
/// }
/// </code>
/// </para>
/// <para>
/// <b>Поиск стадии по полному имени</b> (например, из конфигурации) делается через extension-метод
/// <c>orchestrator.GetStage("catalog:shops")</c> — см. <see cref="JobOrchestratorExtensions.GetStage"/>.
/// </para>
/// </summary>
public interface IJobOrchestrator : IReadOnlyCollection<IDomainHandle> {
	/// <summary>
	/// <c>true</c>, если event loop крашнулся (Faulted). После Faulted-состояния все методы либо возвращают
	/// <see cref="TriggerResult.Faulted"/>, либо бросают <see cref="InvalidOperationException"/>.
	/// </summary>
	bool IsFaulted { get; }

	/// <summary>
	/// Root-домен — фасад для бездоменных (плоских) стадий. Эквивалентно <c>this[<see cref="DomainName.Root"/>]</c>.
	/// </summary>
	IDomainHandle Root { get; }

	/// <summary>
	/// Доступ к домену по имени. <see cref="DomainName.Root"/> (пустая строка) возвращает <see cref="Root"/>.
	/// O(1) hash-lookup в pre-populated кэше; нулевая allocation.
	/// </summary>
	/// <exception cref="ArgumentException">Если домен с таким именем не зарегистрирован.</exception>
	IDomainHandle this[string domainName] { get; }

	/// <summary>
	/// Синхронный снимок состояния всех существующих инстансов всех стадий. Lock-free через atomic-reads
	/// (<see cref="System.Threading.Volatile.Read{T}(ref T)"/>) — eventually consistent между полями одного инстанса,
	/// но всегда без race-conditions на уровне snapshot collection.
	/// </summary>
	InstancesOverview GetOverview();
}
