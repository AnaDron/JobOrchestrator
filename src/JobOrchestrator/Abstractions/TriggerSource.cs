namespace JobOrchestrator.Abstractions;

/// <summary>Источник триггера итерации стадии.</summary>
public enum TriggerSource {
	/// <summary>Запуск по расписанию (внутренний таймер SDK).</summary>
	Auto,
	/// <summary>Запуск через <see cref="IInstanceHandle.RunAsync"/>.</summary>
	Manual,
}
