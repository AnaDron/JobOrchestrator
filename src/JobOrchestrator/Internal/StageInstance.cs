namespace JobOrchestrator.Internal;

/// <summary>
/// Runtime-сущность одного инстанса стадии (long-lived). Несколько <see cref="StageInstance"/> могут
/// разделять один <see cref="StageDescriptor"/> — один на каждый компонент композитного ключа.
/// <para>
/// <b>Метрики</b> (LastSuccess/LastAttempt/ConsecutiveFailures/LastError/NextAutoUtc) хранятся
/// как иммутабельный <see cref="JobMetrics"/> snapshot, заменяемый через
/// <see cref="Interlocked.Exchange{T}(ref T, T)"/>. Это даёт <i>snapshot-consistency</i>: reader-поток
/// (DueScanner, GetOverview) видит согласованную пятёрку полей из одной «эпохи» writer-а, а не
/// торн-сборку из разных моментов между обновлениями. Цена — одна Gen-0 аллокация JobMetrics record
/// на каждое обновление метрик.
/// </para>
/// <para>
/// <see cref="State"/> и <see cref="TryAcquirePendingTick"/> хранятся как отдельные atomic int —
/// они меняются НЕЗАВИСИМО от метрик и должны иметь lock-free CAS (CompareExchange).
/// </para>
/// </summary>
internal sealed class StageInstance {
	public required StageDescriptor Stage { get; init; }
	public required IReadOnlyDictionary<string, string> DependencyKeys { get; init; }
	public required string FullyQualifiedName { get; init; }
	public required string EncodedKey { get; init; }

	/// <summary>Scope для <see cref="IJobStateStore"/>: <c>"{StageName}:{EncodedKey}"</c>.</summary>
	public string StateScope => $"{Stage.Name}:{EncodedKey}";

	JobMetrics _metrics = JobMetrics.Empty;
	int _state;        // 0 = Idle, 1 = Running
	int _pendingTick;  // 0 = свободно, 1 = TimerTickedEvent уже в очереди / обрабатывается

	/// <summary>Атомарный snapshot мутирующихся метрик. Безопасно вызывать из любого потока.</summary>
	public JobMetrics Metrics => Volatile.Read(ref _metrics);

	/// <summary>
	/// Атомарная замена метрик. Используется только из event-loop-consumer-потока (single writer).
	/// Типовой паттерн: <c>instance.SetMetrics(instance.Metrics with { LastSuccess = at, ... })</c>.
	/// </summary>
	public void SetMetrics(JobMetrics next) => Interlocked.Exchange(ref _metrics, next);

	/// <summary>Read-side фасад. Полная семантика см. <see cref="JobMetrics.LastSuccess"/>.</summary>
	public DateTimeOffset? LastSuccess => Metrics.LastSuccess;

	/// <summary>Read-side фасад. Полная семантика см. <see cref="JobMetrics.LastAttempt"/>.</summary>
	public DateTimeOffset? LastAttempt => Metrics.LastAttempt;

	/// <summary>Read-side фасад. Полная семантика см. <see cref="JobMetrics.ConsecutiveFailures"/>.</summary>
	public int ConsecutiveFailures => Metrics.ConsecutiveFailures;

	/// <summary>Read-side фасад. Полная семантика см. <see cref="JobMetrics.LastError"/>.</summary>
	public string? LastError => Metrics.LastError;

	/// <summary>Read-side фасад. Полная семантика см. <see cref="JobMetrics.NextAutoUtc"/>.</summary>
	public DateTimeOffset? NextAutoUtc => Metrics.NextAutoUtc;

	/// <summary>Текущее состояние lifecycle. Хранится отдельно от <see cref="Metrics"/> — меняется независимо.</summary>
	public InstanceLifecycleState State {
		get => Volatile.Read(ref _state) == 0 ? InstanceLifecycleState.Idle : InstanceLifecycleState.Running;
		set => Volatile.Write(ref _state, value == InstanceLifecycleState.Idle ? 0 : 1);
	}

	/// <summary>
	/// Idempotency-CAS для <see cref="DueScanner"/>: <c>true</c> возвращается ровно один раз,
	/// пока <see cref="ReleasePendingTick"/> не сбросит флаг. Защищает от публикации дублирующего
	/// <see cref="TimerTickedEvent"/> на одно и то же due-окно при повторных scan-pass'ах между
	/// первой публикацией и реакцией consumer'а (BeginIteration → State=Running + NextAutoUtc=null).
	/// </summary>
	public bool TryAcquirePendingTick() =>
		Interlocked.CompareExchange(ref _pendingTick, 1, 0) == 0;

	/// <summary>
	/// Сбрасывает pending-tick флаг. Используется DueScanner-ом, если запись в Channel не удалась,
	/// и event-loop-ом по завершению обработки события — в обоих случаях «открывается» следующее
	/// due-окно для публикации очередного TimerTicked.
	/// </summary>
	public void ReleasePendingTick() =>
		Volatile.Write(ref _pendingTick, 0);

	/// <summary>CTS текущей итерации (если State == Running), иначе null.</summary>
	public CancellationTokenSource? RunCts { get; set; }

	/// <summary>Task текущей итерации (если State == Running). Используется для graceful-await при cascade-удалении.</summary>
	public Task? RunningTask { get; set; }
}
