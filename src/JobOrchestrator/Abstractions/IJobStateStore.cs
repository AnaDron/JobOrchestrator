namespace JobOrchestrator.Abstractions;

/// <summary>
/// Pluggable backend для <see cref="IJobState"/>. Хранит значения как непрозрачные строки —
/// сериализация в JSON / Base64 / etc. делается в <see cref="IJobState"/>-реализации поверх этого contract-а.
/// </summary>
/// <remarks>
/// Регистрируется в DI как singleton. SDK поставляет <c>InMemoryJobStateStore</c> в отдельном пакете
/// <c>JobOrchestrator.InMemory</c> (дефолт). Внешние backend-ы (SQL/Redis/file) — отдельные пакеты.
/// </remarks>
public interface IJobStateStore {
	/// <summary>Прочитать значение из указанного scope или <c>null</c>, если ключа нет.</summary>
	Task<string?> GetAsync(string scope, string key, CancellationToken ct);

	/// <summary>Записать значение в указанный scope под ключом.</summary>
	Task SetAsync(string scope, string key, string value, CancellationToken ct);

	/// <summary>Удалить значение. Идемпотентно: отсутствующий ключ — no-op.</summary>
	Task RemoveAsync(string scope, string key, CancellationToken ct);

	/// <summary>
	/// Удалить весь scope целиком. Вызывается SDK при удалении инстанса
	/// (через <see cref="JobContext.RemoveKey"/> или <see cref="IStageHandle.UnregisterKey"/>).
	/// Идемпотентно: отсутствующий scope — no-op.
	/// </summary>
	Task RemoveScopeAsync(string scope, CancellationToken ct);
}
