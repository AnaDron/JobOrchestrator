namespace JobOrchestrator.Abstractions;

/// <summary>Тип события <see cref="StageChange"/> — добавление или удаление инстанса.</summary>
public enum StageChangeKind {
	/// <summary>Инстанс материализован — добавлен в keyspace стадии.</summary>
	Added,

	/// <summary>Инстанс удалён — каскадно по <c>UnregisterKey</c> или ручному вызову.</summary>
	Removed,
}
