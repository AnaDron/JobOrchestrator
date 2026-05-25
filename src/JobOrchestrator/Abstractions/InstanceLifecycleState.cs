namespace JobOrchestrator.Abstractions;

/// <summary>Состояние инстанса стадии.</summary>
/// <remarks>
/// Pending-состояния нет — инстанс физически не существует до момента разрешения всех зависимостей.
/// </remarks>
public enum InstanceLifecycleState {
	/// <summary>Существует, готов к следующему тику или ожидает истечения retry-delay / интервала.</summary>
	Idle,
	/// <summary>Итерация выполняется.</summary>
	Running,
	/// <summary>
	/// Инстанс помечен на удаление (cascade). Если был <see cref="Running"/>, runner-итерация была отменена;
	/// SDK ждёт её фактического завершения, после чего finalize'ит cleanup (<c>IJobStateStore.RemoveScopeAsync</c>
	/// и удаление из <c>InstanceManager</c>). Новые триггеры и due-tick'и для Terminating-инстансов
	/// игнорируются. После finalize инстанс исчезает целиком.
	/// </summary>
	Terminating,
}
