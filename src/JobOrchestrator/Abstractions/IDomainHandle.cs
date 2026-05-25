namespace JobOrchestrator.Abstractions;

/// <summary>
/// Public-fasade для домена — корневая точка доступа к стадиям домена. Получается через
/// <see cref="IJobOrchestrator.this[string]"/> либо <see cref="IJobOrchestrator.Root"/>.
/// <para>
/// Доменом считается префикс полного имени стадии до <c>':'</c>: для <c>"catalog:shops"</c> домен — <c>"catalog"</c>,
/// локальное имя — <c>"shops"</c>. Безымянный домен (root) — <see cref="DomainName.Root"/> — содержит стадии
/// без префикса; полное и локальное имена стадий root-домена совпадают.
/// </para>
/// <para>
/// Frozen on construction: коллекция доменов и набор стадий в каждом — immutable, известны на старте.
/// Lifetime — singleton per-domain внутри <see cref="IJobOrchestrator"/>-реализации (кэшируется).
/// </para>
/// </summary>
public interface IDomainHandle : IReadOnlyCollection<IStageHandle> {
	/// <summary>
	/// Имя домена без separator'а. <see cref="DomainName.Root"/> (пустая строка) — для root-домена;
	/// иначе непустое имя (например, <c>"catalog"</c>).
	/// </summary>
	string Name { get; }

	/// <summary>
	/// Доступ к стадии по локальному имени (без domain-префикса). Полный <see cref="IStageHandle.Name"/>
	/// — это <c>"{Name}:{localStageName}"</c> для не-root домена, либо <c>localStageName</c> для root.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадия с таким локальным именем не зарегистрирована в этом домене.</exception>
	IStageHandle this[string localStageName] { get; }
}
