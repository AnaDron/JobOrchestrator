namespace JobOrchestrator.Abstractions;

/// <summary>
/// Public-fasade для одной стадии. Получается через <see cref="IDomainHandle.this[string]"/>.
/// <para>
/// Frozen on construction (StageDescriptor immutable, runtime-life). Cached один раз на стадию.
/// </para>
/// <para>
/// <b>Перечисление:</b> <see cref="IReadOnlyCollection{IInstanceHandle}"/> отдаёт только материализованные
/// инстансы. Identity-handles, полученные через <see cref="this[InstanceKeys]"/>, в коллекции не отражаются —
/// они переживают как pre-materialization, так и cascade-removal.
/// </para>
/// </summary>
public interface IStageHandle : IReadOnlyCollection<IInstanceHandle> {
	/// <summary>Полное имя стадии: <c>"domain:local"</c> для доменной либо <c>"local"</c> для root.</summary>
	string Name { get; }

	/// <summary>Reverse-link на домен, в котором живёт эта стадия.</summary>
	IDomainHandle Domain { get; }

	/// <summary>
	/// Identity-based доступ к инстансу: handle гарантированно создаётся для любого ключевого набора и
	/// **переживает** как pre-materialization, так и каскадное удаление. Свойства <see cref="IInstanceHandle.State"/>
	/// и <see cref="IInstanceHandle.Snapshot"/> возвращают <c>null</c>, когда инстанса нет на момент чтения.
	/// <para>
	/// Для keyless-стадий используйте <c>stage[<see cref="InstanceKeys.Empty"/>]</c>. Для одиночного ключа —
	/// implicit-конверсия из tuple: <c>stage[("shops", "u1")]</c>.
	/// </para>
	/// </summary>
	/// <exception cref="ArgumentException">
	/// Если переданные key-names не соответствуют <c>ExpectedKeyNames</c> стадии (missing/extra/duplicate).
	/// Fail-fast на handle-construction, чтобы юзер увидел проблему сразу.
	/// </exception>
	IInstanceHandle this[InstanceKeys keys] { get; }

	/// <summary>
	/// Регистрирует ключ в keyspace стадии (внешний bootstrap из БД и т.п.). Идемпотентно.
	/// Работает только для keyless-эмиттеров; для стадий с <c>DependsOnInstance</c>-зависимостями
	/// бросает <see cref="InvalidOperationException"/>.
	/// </summary>
	void RegisterKey(string key);

	/// <summary>Удаляет ключ из keyspace стадии. Каскадно gracefully cancel-ит зависимых.</summary>
	void UnregisterKey(string key);

	/// <summary>
	/// Поток событий keyspace стадии: при подписке сначала эмитируется <see cref="StageChangeKind.Added"/>
	/// для каждого инстанса, существующего на момент подписки (replay), затем live-поток последующих
	/// <see cref="StageChangeKind.Added"/>/<see cref="StageChangeKind.Removed"/>.
	/// <para>
	/// Race-free: подписка и snapshot атомарны через generation-counter — события до момента подписки
	/// фильтруются, потерь между snapshot и live-каналом нет.
	/// </para>
	/// <para>
	/// Per-subscriber bounded buffer с <c>DropOldest</c>: медленный consumer теряет старые события,
	/// drop-счётчик доступен через диагностику (TODO).
	/// </para>
	/// <para>
	/// Stream завершается при shutdown оркестратора либо при cancel переданного <c>CancellationToken</c>.
	/// </para>
	/// </summary>
	IAsyncEnumerable<StageChange> Changes { get; }
}
