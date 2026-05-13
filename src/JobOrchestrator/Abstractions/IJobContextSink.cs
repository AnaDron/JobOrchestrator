namespace JobOrchestrator.Abstractions;

/// <summary>
/// Канал распространения keyspace-операций <c>AddKey</c>/<c>RemoveKey</c> из <see cref="IJobService.ExecuteAsync"/>
/// наружу — в event loop оркестратора. Отделён от <see cref="JobContext"/> для тестируемости:
/// в unit-тестах сервиса можно подменить sink на mock и проверить, какие операции вызывались.
/// <para>
/// В production реализуется внутренним <c>ChannelSink</c>, который публикует <c>KeyAddedEvent</c>/
/// <c>KeyRemovedEvent</c> в <c>Channel&lt;OrchestratorEvent&gt;</c> с привязкой к инстансу-эмитеру.
/// </para>
/// </summary>
public interface IJobContextSink {
	/// <summary>Регистрирует ключ в keyspace текущей стадии. Идемпотентно.</summary>
	void AddKey(string key);

	/// <summary>Удаляет ключ из keyspace текущей стадии. Каскадно отменяет зависимых. Идемпотентно.</summary>
	void RemoveKey(string key);
}
