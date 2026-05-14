namespace JobOrchestrator.Internal;

/// <summary>
/// Runtime-сущность одного инстанса стадии (long-lived). Несколько <see cref="Instance"/> могут
/// разделять один <see cref="StageDescriptor"/> — один на каждый компонент композитного ключа.
/// <para>
/// <b>Identity</b> — иммутабельный идентификатор; <b>Sink</b> pre-allocated per lifetime.
/// </para>
/// <para>
/// <b>Состояние</b> хранится двумя независимыми atomic-int-флагами:
/// </para>
/// <list type="bullet">
/// <item><c>_running</c> (0/1): CAS через <see cref="TryBeginRunning"/> / clear через <see cref="EndRunning"/>.</item>
/// <item><c>_terminating</c> (0/1): CAS через <see cref="MarkTerminating"/> — возвращает <c>true</c> ровно один раз.</item>
/// </list>
/// <para>
/// Эти флаги <b>не взаимоисключающие</b>: инстанс может быть Running И Terminating одновременно
/// (runner отменён, но ещё не закончил отстреливать <see cref="StageCompletedEvent"/>/<see cref="StageFailedEvent"/>).
/// <see cref="State"/> сводит их в публичный enum по приоритету Terminating &gt; Running &gt; Idle.
/// </para>
/// <para>
/// Метрики (<see cref="Metrics"/>) — иммутабельный <see cref="JobMetrics"/> snapshot, заменяемый
/// через <see cref="Interlocked.Exchange"/>. Facade-доступ к отдельным полям отсутствует — call-sites
/// явно делают <c>instance.Metrics</c>, что видно как optimization-hotspot.
/// </para>
/// </summary>
internal sealed class Instance {
	public required InstanceIdentity Identity { get; init; }

	/// <summary>
	/// Sink для пересылки <c>ctx.AddKey/RemoveKey</c> в event loop. Pre-allocated в InstanceCreator,
	/// переиспользуется через все итерации.
	/// </summary>
	public IJobContextSink Sink { get; set; } = null!;

	// Identity-facades — immutable, делегируют для краткости call-sites.
	public StageDescriptor Stage => Identity.Stage;
	public IReadOnlyDictionary<string, string> DependencyKeys => Identity.DependencyKeys;
	public string FullyQualifiedName => Identity.FullyQualifiedName;
	public string EncodedKey => Identity.EncodedKey;
	public string StateScope => Identity.StateScope;

	JobMetrics _metrics = JobMetrics.Empty;
	int _running;       // 0 = не Running, 1 = Running
	int _terminating;   // 0 = не Terminating, 1 = Terminating
	int _pendingTick;
	volatile CancellationTokenSource? _runCts;

	/// <summary>Атомарный snapshot мутирующихся метрик. Безопасно вызывать из любого потока.</summary>
	public JobMetrics Metrics => Volatile.Read(ref _metrics);

	/// <summary>Атомарная замена метрик. Используется только из event-loop-consumer-потока.</summary>
	public void SetMetrics(JobMetrics next) => Interlocked.Exchange(ref _metrics, next);

	/// <summary>
	/// Публичное состояние lifecycle — производное от двух флагов. Приоритет:
	/// Terminating > Running > Idle. В <see cref="InstancesOverview"/> оператор видит Terminating
	/// для инстансов, ожидающих cleanup.
	/// </summary>
	public InstanceLifecycleState State {
		get {
			if (Volatile.Read(ref _terminating) == 1) return InstanceLifecycleState.Terminating;
			return Volatile.Read(ref _running) == 1 ? InstanceLifecycleState.Running : InstanceLifecycleState.Idle;
		}
	}

	/// <summary>True, если инстанс находится в active-iteration (runner запущен и не закончил).</summary>
	public bool IsRunning => Volatile.Read(ref _running) == 1;

	/// <summary>True, если инстанс помечен на удаление каскадом.</summary>
	public bool IsTerminating => Volatile.Read(ref _terminating) == 1;

	/// <summary>
	/// CAS-перевод в Running. Возвращает <c>true</c>, если переход состоялся (т.е. был Idle до этого);
	/// <c>false</c>, если уже Running. Используется в <c>EventLoop.BeginIteration</c> для идемпотентности.
	/// </summary>
	public bool TryBeginRunning() => Interlocked.CompareExchange(ref _running, 1, 0) == 0;

	/// <summary>
	/// Снимает Running-флаг + pendingTick. Вызывается из <c>StageRunner.RunIterationAsync</c> finally —
	/// гарантирует выход из Running при любом исходе итерации (success/failure/cancel/exception).
	/// </summary>
	public void EndRunning() {
		Volatile.Write(ref _pendingTick, 0);
		Volatile.Write(ref _running, 0);
	}

	/// <summary>
	/// CAS-перевод в Terminating. Возвращает <c>true</c> ровно один раз (первый вызов).
	/// Каскад использует это для идемпотентности: если попытка отметить тот же инстанс через два разных
	/// orphan-ключа — второй <c>MarkTerminating</c> вернёт <c>false</c>, caller пропускает.
	/// </summary>
	public bool MarkTerminating() => Interlocked.CompareExchange(ref _terminating, 1, 0) == 0;

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
	/// CTS текущей итерации (если IsRunning), иначе null. <c>volatile</c>: writer (runner finally) и
	/// reader (event-loop cascade-cancel) должны видеть согласованные записи без отдельных Interlocked.
	/// </summary>
	public CancellationTokenSource? RunCts {
		get => _runCts;
		set => _runCts = value;
	}

	/// <summary>
	/// Атомарно сбрасывает <see cref="RunCts"/> в null ТОЛЬКО если текущее значение совпадает с
	/// <paramref name="expected"/>. Используется в <c>StageRunner</c> finally — защищает от случая,
	/// когда после <c>EndRunning</c> уже стартовал следующий runner и установил свой CTS:
	/// «свой» runner не должен затирать чужую запись.
	/// </summary>
	public void ClearRunCtsIfEquals(CancellationTokenSource expected) =>
		Interlocked.CompareExchange(ref _runCts, null, expected);
}
