namespace JobOrchestrator.Abstractions;

/// <summary>
/// Точка управления оркестратором снаружи: ручные триггеры, наполнение keyspace для bootstrap, диагностика.
/// Резолвится из DI как singleton.
/// </summary>
public interface IJobOrchestrator {
	/// <summary>
	/// <c>true</c>, если event loop крашнулся (Faulted). После Faulted-состояния все методы либо возвращают
	/// <see cref="TriggerResult.Faulted"/>, либо бросают <see cref="InvalidOperationException"/>.
	/// </summary>
	bool IsFaulted { get; }

	/// <summary>
	/// Запросить ручной запуск инстанса. Подчиняется логике <c>TryAcceptTrigger</c>: Manual игнорирует retry-delay,
	/// но уважает debounce-окно (от LastAttempt, вне зависимости от исхода последней попытки).
	/// </summary>
	/// <param name="stageName">Имя стадии (как задано в Fluent API).</param>
	/// <param name="dependencyKeys">
	/// Композитный ключ целевого инстанса. <c>null</c> или пустой словарь — для безключевых стадий.
	/// Набор имён должен соответствовать набору <c>DependsOnInstance</c>-зависимостей стадии,
	/// иначе возвращается <see cref="TriggerResult.InvalidKeys"/>.
	/// </param>
	Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default);

	/// <summary>
	/// Регистрирует ключ в keyspace указанной стадии снаружи (вне её собственного <see cref="JobContext.AddKey"/>).
	/// Идемпотентно. Используется для bootstrap (например, наполнение keyspace из БД при старте приложения).
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не зарегистрирована в графе.</exception>
	/// <exception cref="InvalidOperationException">Если оркестратор в Faulted-состоянии.</exception>
	void RegisterKey(string stageName, string key);

	/// <summary>
	/// Удаляет ключ из keyspace указанной стадии снаружи. Идемпотентно. Каскадно gracefully cancel-ит
	/// инстансы зависимых стадий с этим компонентом ключа (топологически обратный порядок).
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не зарегистрирована в графе.</exception>
	/// <exception cref="InvalidOperationException">Если оркестратор в Faulted-состоянии.</exception>
	void UnregisterKey(string stageName, string key);

	/// <summary>
	/// Синхронный снимок состояния всех существующих инстансов всех стадий. Lock-free через atomic-reads
	/// (<see cref="System.Threading.Volatile.Read{T}(ref T)"/>) — eventually consistent между полями одного инстанса,
	/// но всегда без race-conditions на уровне snapshot collection.
	/// </summary>
	InstancesOverview GetOverview();

	/// <summary>
	/// Завершается, когда инстанс <paramref name="stageName"/> с указанными ключами успешно
	/// отработает хотя бы один раз. Memoized: если на момент вызова <c>LastSuccess != null</c> —
	/// возвращает уже-завершённый <see cref="Task"/>. Если инстанс ещё не создан, ожидание висит
	/// до его появления и первого успеха. Если инстанс удалён каскадом до первого успеха —
	/// Task завершается <see cref="InvalidOperationException"/>. Отмена через <paramref name="ct"/> →
	/// <see cref="OperationCanceledException"/>. При shutdown оркестратора — <see cref="InvalidOperationException"/>.
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не в графе или keys не соответствуют ExpectedKeyNames.</exception>
	Task WaitForStageSuccessAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default);

	/// <summary>
	/// Завершается на ПЕРВОМ ИСХОДЕ следующего цикла стадии: <see cref="StageOutcomeKind.Success"/>,
	/// <see cref="StageOutcomeKind.Failure"/> или <see cref="StageOutcomeKind.Cancelled"/>
	/// (инстанс удалён каскадом до отработки). Регистрация выполняется синхронно — caller сам отвечает
	/// за порядок «Register перед Trigger», если хочет ждать именно текущий запуск.
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не в графе или keys не соответствуют ExpectedKeyNames.</exception>
	Task<StageOutcome> WaitForStageOutcomeAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default);
}
