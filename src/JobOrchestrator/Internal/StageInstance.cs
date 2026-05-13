namespace JobOrchestrator.Internal;

/// <summary>
/// Runtime-сущность одного инстанса стадии (long-lived). Несколько <see cref="StageInstance"/> могут
/// разделять один <see cref="StageDescriptor"/> — один на каждый компонент композитного ключа.
/// <para>
/// <b>Identity</b> (<see cref="Identity"/>) — иммутабельный идентификатор инстанса
/// <c>(Stage, DependencyKeys)</c> с pre-computed <c>EncodedKey</c> и <c>FullyQualifiedName</c>.
/// Все hash-lookup'ы (<see cref="InstanceManager"/>) и log-fields идут через него.
/// Identity-facades (<see cref="Stage"/>, <see cref="DependencyKeys"/>, <see cref="FullyQualifiedName"/>,
/// <see cref="EncodedKey"/>, <see cref="StateScope"/>) — простые делегаты, без читательских проблем.
/// </para>
/// <para>
/// <b>Метрики</b> (<see cref="Metrics"/>) — иммутабельный <see cref="JobMetrics"/> snapshot, заменяемый
/// через <see cref="Interlocked.Exchange{T}(ref T, T)"/>. Facade-доступа к отдельным полям метрик
/// (LastSuccess, LastAttempt, и т.д.) <b>намеренно нет</b> — call-sites обязаны явно делать
/// <c>instance.Metrics</c> (один <c>Volatile.Read</c>) и потом читать поля snapshot'а. Это сразу
/// показывает места, где Metrics читается несколько раз → можно оптимизировать локальной переменной.
/// </para>
/// <para>
/// <b>State</b> (<see cref="State"/>) и <see cref="TryAcquirePendingTick"/> — отдельные atomic int,
/// меняются независимо от метрик. <see cref="RunCts"/> — <c>volatile</c>-reference для безопасного
/// чтения writer'ом (runner-thread) и reader'ом (event-loop).
/// </para>
/// </summary>
internal sealed class StageInstance {
	public required InstanceIdentity Identity { get; init; }

	// Identity-facades — immutable, без подводных камней; делегируют для краткости call-sites.
	public StageDescriptor Stage => Identity.Stage;
	public IReadOnlyDictionary<string, string> DependencyKeys => Identity.DependencyKeys;
	public string FullyQualifiedName => Identity.FullyQualifiedName;
	public string EncodedKey => Identity.EncodedKey;
	public string StateScope => Identity.StateScope;

	JobMetrics _metrics = JobMetrics.Empty;
	int _state;        // 0 = Idle, 1 = Running, 2 = Terminating
	int _pendingTick;  // 0 = свободно, 1 = TimerTickedEvent уже в очереди / обрабатывается
	volatile CancellationTokenSource? _runCts;

	/// <summary>Атомарный snapshot мутирующихся метрик. Безопасно вызывать из любого потока.</summary>
	public JobMetrics Metrics => Volatile.Read(ref _metrics);

	/// <summary>
	/// Атомарная замена метрик. Используется только из event-loop-consumer-потока (single writer).
	/// Типовой паттерн: <c>instance.SetMetrics(instance.Metrics with { LastSuccess = at, ... })</c>.
	/// </summary>
	public void SetMetrics(JobMetrics next) => Interlocked.Exchange(ref _metrics, next);

	/// <summary>Текущее состояние lifecycle. Хранится отдельно от <see cref="Metrics"/> — меняется независимо.</summary>
	public InstanceLifecycleState State {
		get => (InstanceLifecycleState)Volatile.Read(ref _state);
		set => Volatile.Write(ref _state, (int)value);
	}

	/// <summary>
	/// Idempotency-CAS для <see cref="DueScanner"/>: <c>true</c> возвращается ровно один раз,
	/// пока <see cref="ReleasePendingTick"/> не сбросит флаг.
	/// </summary>
	public bool TryAcquirePendingTick() =>
		Interlocked.CompareExchange(ref _pendingTick, 1, 0) == 0;

	/// <summary>Сбрасывает pending-tick флаг.</summary>
	public void ReleasePendingTick() =>
		Volatile.Write(ref _pendingTick, 0);

	/// <summary>
	/// CTS текущей итерации (если State == Running), иначе null. <c>volatile</c>: writer (runner finally
	/// в одном потоке) и reader (event-loop cascade-cancel в другом потоке) должны видеть согласованные
	/// записи без отдельных Interlocked-операций.
	/// </summary>
	public CancellationTokenSource? RunCts {
		get => _runCts;
		set => _runCts = value;
	}
}
