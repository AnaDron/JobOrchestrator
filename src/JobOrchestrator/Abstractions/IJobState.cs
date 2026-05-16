namespace JobOrchestrator.Abstractions;

/// <summary>
/// Нейтральный key-value bag для хранения произвольного состояния итераций инстанса (cursor, watermark, last-sync-токен).
/// SDK не интерпретирует содержимое — сериализация и схема целиком на стороне БЛ.
/// </summary>
/// <remarks>
/// Scope автоматически изолирован per-инстанс: каждый инстанс с уникальным <see cref="JobContext.FullyQualifiedName"/>
/// получает свой scope в <see cref="IJobStateStore"/>. При удалении инстанса (через <see cref="JobContext.RemoveKeyAsync"/>
/// или <see cref="IStageHandle.UnregisterKey"/>) scope полностью очищается через
/// <see cref="IJobStateStore.RemoveScopeAsync"/>.
/// </remarks>
public interface IJobState {
	/// <summary>Прочитать значение или <c>null</c>, если ключа нет в scope.</summary>
	Task<T?> GetAsync<T>(string key, CancellationToken ct = default);

	/// <summary>Записать значение по ключу. Перезаписывает существующее.</summary>
	Task SetAsync<T>(string key, T value, CancellationToken ct = default);

	/// <summary>Удалить значение по ключу. Идемпотентно: отсутствующий ключ — no-op.</summary>
	Task RemoveAsync(string key, CancellationToken ct = default);
}
