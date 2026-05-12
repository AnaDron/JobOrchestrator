namespace JobOrchestrator.Configuration;

/// <summary>Ошибка конфигурации графа стадий: цикл, висячая зависимость, дубль имён, отсутствие <see cref="IJobService"/>-реализации в DI.</summary>
public sealed class JobConfigurationException : Exception {
	public JobConfigurationException(string message) : base(message) { }
	public JobConfigurationException(string message, Exception inner) : base(message, inner) { }
}
