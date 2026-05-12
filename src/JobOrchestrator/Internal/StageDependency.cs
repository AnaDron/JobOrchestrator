namespace JobOrchestrator.Internal;

/// <summary>Тип зависимости между стадиями.</summary>
internal enum DependencyMode {
	/// <summary>DependsOn — fan-out: для каждого успешного инстанса родителя порождается инстанс зависимой стадии с теми же ключами.</summary>
	Whole,
	/// <summary>DependsOnInstance — keyspace-driven: для каждого ключа в keyspace родителя создаётся инстанс с компонентом ключа от этого родителя.</summary>
	Instance,
}

/// <summary>Описание одной зависимости стадии. Порядок в <see cref="StageDescriptor.Dependencies"/> = порядок объявления в Fluent API.</summary>
internal sealed record StageDependency(string TargetStageName, DependencyMode Mode);
