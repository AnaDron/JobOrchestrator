namespace JobOrchestrator.Configuration;

/// <summary>Fluent API одной стадии.</summary>
public interface IStageBuilder {
	/// <summary>Имя стадии (как задано в <c>JobOrchestratorBuilder.Stage(name)</c>).</summary>
	string Name { get; }

	/// <summary>Указать реализацию <see cref="IJobService"/>. Тип резолвится из DI-scope per итерация.</summary>
	IStageBuilder HandledBy<TService>() where TService : class, IJobService;

	/// <summary>Расписание Auto-тика: интервал между запусками после успешного завершения.</summary>
	IStageBuilder RunPeriodically(TimeSpan interval);

	/// <summary>Политика расчёта retry-delay после неуспеха.</summary>
	IStageBuilder RetryAfterFailure(RetryPolicy policy);

	/// <summary>Окно дебаунса для Manual-триггера (от <c>LastAttempt</c>, всегда).</summary>
	IStageBuilder Debounce(TimeSpan window);

	/// <summary>Watchdog-таймаут одной итерации: linked CTS с <c>CancelAfter</c>.</summary>
	IStageBuilder WithExecutionTimeout(TimeSpan timeout);

	/// <summary>
	/// Лимит одновременно работающих инстансов этой стадии. Для multi-instance-стадий (с
	/// <see cref="DependsOnInstance"/>) защищает от saturate-а downstream-ресурсов (DB connection pool,
	/// внешний API rate-limit). По умолчанию лимита нет (все инстансы могут выполняться параллельно).
	/// <para>
	/// Реализация — <c>SemaphoreSlim</c> per stage: <c>BeginIteration</c> ждёт acquire-токена,
	/// release происходит в <c>finally</c> блока <see cref="EventLoop.RunIterationAsync"/> —
	/// гарантирует release при любом исходе (success/failure/cancel).
	/// </para>
	/// </summary>
	IStageBuilder WithConcurrencyLimit(int maxParallel);

	/// <summary>
	/// Whole-зависимость: для каждого успешно завершённого инстанса <paramref name="other"/>
	/// создаётся соответствующий инстанс этой стадии с теми же DependencyKeys.
	/// Семантика «fan-out с наследованием ключей».
	/// </summary>
	IStageBuilder DependsOn(IStageBuilder other);

	/// <summary>
	/// Instance-зависимость: на каждый ключ <c>k ∈ keyspace(other)</c> создаётся инстанс этой стадии
	/// с компонентом <c>{other.Name: k}</c> в DependencyKeys. Семантика «keyspace-driven introduction нового измерения ключа».
	/// </summary>
	IStageBuilder DependsOnInstance(IStageBuilder other);
}
