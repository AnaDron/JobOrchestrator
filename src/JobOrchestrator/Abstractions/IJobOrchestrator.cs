namespace JobOrchestrator.Abstractions;

/// <summary>
/// Точка управления оркестратором снаружи: ручные триггеры, наполнение keyspace для bootstrap, диагностика.
/// Резолвится из DI как singleton.
/// <para>
/// <b>Handle-API:</b> <c>orchestrator["stageName"][InstanceKey.None | (key, value), ...].Operation()</c>
/// — структурированный доступ через <see cref="IStageHandle"/> → <see cref="IInstanceHandle"/>.
/// Старые flat-методы (<c>TriggerAsync(stageName, dict, ct)</c>, <c>WaitForStage*Async</c>) помечены
/// <c>[Obsolete]</c> и будут удалены в следующей мажорной версии.
/// </para>
/// <para>
/// <b>Enumeration:</b> <see cref="IJobOrchestrator"/> сам по себе является <see cref="IEnumerable{IStageHandle}"/>:
/// <c>foreach (var stage in orchestrator) { ... }</c> — итерация всех зарегистрированных стадий.
/// </para>
/// <para>
/// <b>Кэширование handles в hot-path:</b> indexer-вызовы создают per-call аллокации (InstanceHandle + Identity).
/// Для polling-сценариев (UI-обновления, метрики) — кэшируйте handle локально:
/// <code>
/// var h = orchestrator["pg"][("shops", "u1")];   // один раз
/// while (running) {
///     var state = h.State;                        // re-uses cached identity
///     await Task.Delay(...);
/// }
/// </code>
/// </para>
/// </summary>
public interface IJobOrchestrator : IEnumerable<IStageHandle> {
	/// <summary>
	/// <c>true</c>, если event loop крашнулся (Faulted). После Faulted-состояния все методы либо возвращают
	/// <see cref="TriggerResult.Faulted"/>, либо бросают <see cref="InvalidOperationException"/>.
	/// </summary>
	bool IsFaulted { get; }

	/// <summary>
	/// Root handle-API: <c>orchestrator["stageName"]</c> → <see cref="IStageHandle"/>.
	/// O(1) hash-lookup в pre-populated кеше; нулевая allocation.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадия не зарегистрирована в графе.</exception>
	IStageHandle this[string stageName] { get; }

	/// <summary>
	/// Синхронный снимок состояния всех существующих инстансов всех стадий. Lock-free через atomic-reads
	/// (<see cref="System.Threading.Volatile.Read{T}(ref T)"/>) — eventually consistent между полями одного инстанса,
	/// но всегда без race-conditions на уровне snapshot collection.
	/// </summary>
	InstancesOverview GetOverview();

	#region [Obsolete] flat-API — заменён на handle-API, удалён в следующей мажорной версии

	/// <summary>
	/// Запросить ручной запуск инстанса. Подчиняется логике <c>TryAcceptTrigger</c>: Manual игнорирует retry-delay,
	/// но уважает debounce-окно (от LastAttempt, вне зависимости от исхода последней попытки).
	/// </summary>
	[Obsolete("Используйте orchestrator[stageName][keys].TriggerAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB001")]
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
	[Obsolete("Используйте orchestrator[stageName].RegisterKey(key). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB004")]
	void RegisterKey(string stageName, string key);

	/// <summary>
	/// Удаляет ключ из keyspace указанной стадии снаружи. Идемпотентно. Каскадно gracefully cancel-ит
	/// инстансы зависимых стадий с этим компонентом ключа (топологически обратный порядок).
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не зарегистрирована в графе.</exception>
	/// <exception cref="InvalidOperationException">Если оркестратор в Faulted-состоянии.</exception>
	[Obsolete("Используйте orchestrator[stageName].UnregisterKey(key). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB005")]
	void UnregisterKey(string stageName, string key);

	/// <summary>
	/// Завершается, когда инстанс <paramref name="stageName"/> с указанными ключами успешно
	/// отработает хотя бы один раз. Memoized: если на момент вызова <c>LastSuccess != null</c> —
	/// возвращает уже-завершённый <see cref="Task"/>. Если инстанс ещё не создан, ожидание висит
	/// до его появления и первого успеха. Если инстанс удалён каскадом до первого успеха —
	/// Task завершается <see cref="InvalidOperationException"/>. Отмена через <paramref name="ct"/> →
	/// <see cref="OperationCanceledException"/>. При shutdown оркестратора — <see cref="InvalidOperationException"/>.
	/// </summary>
	/// <exception cref="ArgumentException">Если <paramref name="stageName"/> не в графе или keys не соответствуют ExpectedKeyNames.</exception>
	[Obsolete("Используйте orchestrator[stageName][keys].WaitForSuccessAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB002")]
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
	[Obsolete("Используйте orchestrator[stageName][keys].WaitForOutcomeAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB003")]
	Task<StageOutcome> WaitForStageOutcomeAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default);

	#endregion
}
