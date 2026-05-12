namespace JobOrchestrator.Abstractions;

/// <summary>Состояние инстанса стадии.</summary>
/// <remarks>
/// Pending-состояния нет — инстанс физически не существует до момента разрешения всех зависимостей.
/// </remarks>
public enum JobLifecycleState {
	/// <summary>Существует, готов к следующему тику или ожидает истечения retry-delay / интервала.</summary>
	Idle,
	/// <summary>Итерация выполняется.</summary>
	Running,
}
