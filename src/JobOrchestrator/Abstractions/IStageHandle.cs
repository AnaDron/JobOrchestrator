namespace JobOrchestrator.Abstractions;

/// <summary>
/// Public-fasade для одной стадии — корневая точка для доступа к её инстансам и keyspace.
/// Получается через <see cref="IJobOrchestrator"/>-индексатор: <c>orchestrator["stageName"]</c>.
/// <para>
/// Cached один раз на стадию (StageDescriptor immutable, runtime-life). Перевод на handle стоит
/// одного hash-lookup в indexer-call <see cref="IJobOrchestrator.this"/>.
/// </para>
/// </summary>
public interface IStageHandle {
	/// <summary>Имя стадии.</summary>
	string Name { get; }

	/// <summary>Keyless-инстанс (для стадий без <c>DependsOnInstance</c>-зависимостей).</summary>
	IInstanceHandle this[InstanceKey marker] { get; }

	/// <summary>Инстанс с одной key-component (наиболее частый случай).</summary>
	IInstanceHandle this[(string Name, string Value) key] { get; }

	/// <summary>Инстанс с двумя key-components.</summary>
	IInstanceHandle this[(string Name, string Value) key1, (string Name, string Value) key2] { get; }

	/// <summary>
	/// Инстанс с произвольным числом key-components. <see cref="ReadOnlySpan{T}"/>-params:
	/// для small N компилятор эмитит stack-аллокацию (zero-heap); для большего — fallback на heap-массив.
	/// </summary>
	IInstanceHandle this[params ReadOnlySpan<(string Name, string Value)> keys] { get; }

	/// <summary>
	/// Регистрирует ключ в keyspace стадии (внешний bootstrap из БД и т.п.). Идемпотентно.
	/// Работает только для keyless-эмитеров; для стадий с <c>DependsOnInstance</c>-зависимостями
	/// бросает <see cref="InvalidOperationException"/>.
	/// </summary>
	void RegisterKey(string key);

	/// <summary>Удаляет ключ из keyspace стадии. Каскадно gracefully cancel-ит зависимых.</summary>
	void UnregisterKey(string key);

	/// <summary>Снимок всех текущих инстансов этой стадии. Lock-free.</summary>
	IReadOnlyList<IInstanceHandle> AllInstances { get; }
}
