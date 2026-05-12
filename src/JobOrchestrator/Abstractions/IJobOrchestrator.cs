namespace JobOrchestrator.Abstractions;

/// <summary>
/// Точка управления оркестратором снаружи: ручные триггеры, наполнение keyspace для bootstrap, диагностика.
/// Резолвится из DI как singleton.
/// </summary>
public interface IJobOrchestrator {
	/// <summary>
	/// <c>true</c>, если event loop крашнулся (Faulted), либо сервис ещё не стартовал. После Faulted-состояния
	/// все методы либо возвращают <see cref="TriggerResult.Faulted"/>, либо бросают <see cref="InvalidOperationException"/>.
	/// </summary>
	bool IsFaulted { get; }

	/// <summary>
	/// Запросить ручной запуск инстанса. Подчиняется логике <c>TryAcceptTrigger</c>: Manual игнорирует retry-delay,
	/// но уважает debounce-окно (от LastAttempt, вне зависимости от исхода последней попытки).
	/// </summary>
	/// <param name="stageName">Имя стадии (как задано в Fluent API).</param>
	/// <param name="dependencyKeys">
	/// Композитный ключ целевого инстанса. <c>null</c> или пустой словарь — для безключевых стадий.
	/// </param>
	Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default);

	/// <summary>
	/// Регистрирует ключ в keyspace указанной стадии снаружи (вне её собственного <see cref="JobContext.AddKey"/>).
	/// Идемпотентно. Используется для bootstrap (например, наполнение keyspace из БД при старте приложения).
	/// </summary>
	/// <exception cref="InvalidOperationException">Если оркестратор в Faulted-состоянии.</exception>
	void RegisterKey(string stageName, string key);

	/// <summary>
	/// Удаляет ключ из keyspace указанной стадии снаружи. Идемпотентно. Каскадно gracefully cancel-ит
	/// инстансы зависимых стадий с этим компонентом ключа (топологически обратный порядок).
	/// </summary>
	/// <exception cref="InvalidOperationException">Если оркестратор в Faulted-состоянии.</exception>
	void UnregisterKey(string stageName, string key);

	/// <summary>
	/// Снимок состояния всех существующих инстансов всех стадий. Запрос проходит через event loop —
	/// возвращает консистентный снимок без race-conditions с конкурентными изменениями состояния.
	/// </summary>
	Task<InstancesOverview> GetOverviewAsync(CancellationToken ct = default);
}
