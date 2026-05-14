namespace JobOrchestrator.Internal;

/// <summary>Тип зависимости между стадиями.</summary>
internal enum DependencyMode {
	/// <summary>DependsOn — fan-out: для каждого успешного инстанса родителя порождается инстанс зависимой стадии с теми же ключами.</summary>
	Whole,
	/// <summary>DependsOnInstance — keyspace-driven: для каждого ключа в keyspace родителя создаётся инстанс с компонентом ключа от этого родителя.</summary>
	Instance,
}

/// <summary>
/// Описание одной зависимости стадии. Порядок в <see cref="StageDescriptor.Dependencies"/> = порядок объявления в Fluent API.
/// <para>
/// <see cref="Target"/> — прямая ссылка на дескриптор родителя (а не имя-строка): граф самодостаточен
/// в дескрипторах, lookup через <see cref="StageRegistry"/> по имени для resolve не требуется.
/// Forward-references разруливаются в <see cref="StageInitializer"/>.<c>DependenciesOf</c>: он
/// получает уже-существующий registry и резолвит имена в descriptor-refs.
/// </para>
/// </summary>
internal sealed record StageDependency(StageDescriptor Target, DependencyMode Mode);
