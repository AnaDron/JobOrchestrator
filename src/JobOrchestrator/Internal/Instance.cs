using System.Collections.Concurrent;

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
/// Метрики (<see cref="Metrics"/>) — иммутабельный <see cref="JobMetrics"/> snapshot
/// (<see cref="InstanceExecutionStats"/> + <see cref="InstanceSchedule"/>), заменяемый
/// через <see cref="Interlocked.Exchange"/>. Facade-доступ к отдельным полям отсутствует — call-sites
/// явно делают <c>instance.Metrics</c>, что видно как optimization-hotspot.
/// </para>
/// </summary>
internal sealed class Instance {
	public required InstanceIdentity Identity { get; init; }

	/// <summary>
	/// Sink для пересылки <c>ctx.AddKeyAsync/RemoveKeyAsync</c> в event loop. Pre-allocated в InstanceCreator,
	/// переиспользуется через все итерации.
	/// </summary>
	public IJobContextSink Sink { get; set; } = null!;

	// Identity-facades — immutable, делегируют для краткости call-sites.
	public StageDescriptor Stage => Identity.Stage;
	public IReadOnlyDictionary<string, string> DependencyKeys => Identity.DependencyKeys;
	public InstanceKeys Keys => Identity.Keys;
	public string FullyQualifiedName => Identity.FullyQualifiedName;

	// Lazy-кеш: StateScope — application-концерн (ключ state store), не часть Identity.
	// Вычисляется один раз при первом обращении; обе части (Stage.Name, EncodedKey) уже кешированы в Identity.
	private string? _stateScope;
	public string StateScope => _stateScope ??= $"{Identity.Stage.Name}:{Identity.EncodedKey}";

	JobMetrics _metrics = JobMetrics.Empty;
	int _running;       // 0 = не Running, 1 = Running
	int _terminating;   // 0 = не Terminating, 1 = Terminating
	int _pendingTick;
	volatile CancellationTokenSource? _runCts;
	TaskCompletionSource? _iterationOutcomeTcs;
	// Keyspace эмитера: ключи, опубликованные этим инстансом через JobContext.AddKeyAsync /
	// IStageHandle.RegisterKey. Plain HashSet — мутации только на event-loop-consumer-потоке
	// (HandleKeyAdded/HandleKeyRemovedAsync/CascadeKeyRemovalAsync), читает только event-loop
	// (через InstanceCreator.ComputeDimension), без concurrent-обёртки.
	private readonly HashSet<string> _emittedKeys = new(StringComparer.Ordinal);
	private const int IterationBufferCapacity = 64;

	private volatile IIterationHandle? _runningIteration;
	// CHM-as-Set: subscriber add/remove из caller-потоков concurrent с notify-iteration из event-loop —
	// нужен lock-free thread-safe set. В BCL ConcurrentHashSet<T> отсутствует, value=byte неиспользуется.
	private readonly ConcurrentDictionary<BoundedSubscriber<IIterationHandle>, byte> _iterationSubscribers = new();

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
	/// Снимает Running-флаг + pendingTick + RunningIteration. Вызывается из <see cref="EventLoop"/>
	/// (<c>HandleStageCompletedAsync</c>/<c>HandleStageFailedAsync</c>, <c>DrainPendingRequests</c>)
	/// или из <c>EventLoop.RunIterationAsync</c>'s <c>finally</c>, если completion не опубликован в channel.
	/// </summary>
	public void EndRunning() {
		Volatile.Write(ref _pendingTick, 0);
		Volatile.Write(ref _running, 0);
		_runningIteration = null;
	}

	/// <summary>
	/// CAS-перевод в Terminating. Возвращает <c>true</c> ровно один раз (первый вызов).
	/// Каскад использует это для идемпотентности: если попытка отметить тот же инстанс через два разных
	/// orphan-ключа — второй <c>MarkTerminating</c> вернёт <c>false</c>, caller пропускает.
	/// </summary>
	public bool MarkTerminating() => Interlocked.CompareExchange(ref _terminating, 1, 0) == 0;

	/// <summary>
	/// Idempotency-CAS для <see cref="EventLoop"/>: <c>true</c> возвращается ровно один раз,
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
	/// <paramref name="expected"/>. Используется в <c>EventLoop.RunIterationAsync</c> finally — защищает от случая,
	/// когда после <c>EndRunning</c> уже стартовал следующий runner и установил свой CTS:
	/// «свой» runner не должен затирать чужую запись.
	/// </summary>
	public void ClearRunCtsIfEquals(CancellationTokenSource expected) =>
		Interlocked.CompareExchange(ref _runCts, null, expected);

	/// <summary>
	/// Текущая активная итерация (running), либо <c>null</c> когда инстанс Idle/Terminating. Volatile-read
	/// для consistency между event-loop-consumer-потоком (writer) и polling-сценариями
	/// (<see cref="IInstanceHandle.RunningIteration"/>, читатели — любые потоки).
	/// </summary>
	public IIterationHandle? RunningIteration => _runningIteration;

	/// <summary>
	/// Регистрирует только что начавшуюся итерацию: привязывает iteration-scoped TCS (резолвится в
	/// completion-handler'е через <see cref="TakeIterationOutcomeTcs"/>), выставляет
	/// <see cref="RunningIteration"/>, и публикует handle всем подписчикам iteration-broadcaster'а.
	/// <para>
	/// Single-writer: вызывается только из event-loop-consumer-потока в <c>EventLoop.BeginIteration</c>
	/// сразу после CAS-перевода в Running. Конкурентного writer'а не существует — параллельная итерация
	/// на том же инстансе невозможна.
	/// </para>
	/// <para>
	/// <b>Race-free инвариант:</b> Runner запускается на ThreadPool в <c>BeginIteration</c>; даже если он
	/// завершится моментально и опубликует <c>StageCompletedEvent</c> в channel, event-loop не обработает
	/// это событие до возврата из <c>HandleManualTrigger</c> (single-threaded consumer). Поэтому к моменту,
	/// когда <c>HandleStageCompletedAsync</c> зовёт <see cref="TakeIterationOutcomeTcs"/>, TCS гарантированно
	/// уже привязан.
	/// </para>
	/// </summary>
	public void OnIterationStarted(IIterationHandle handle, TaskCompletionSource outcomeTcs) {
		_iterationOutcomeTcs = outcomeTcs;
		_runningIteration = handle;
		foreach (var sub in _iterationSubscribers.Keys) sub.Publish(handle);
	}

	/// <summary>
	/// Атомарно снимает iteration-outcome TCS (set to null) и возвращает прежнее значение, либо
	/// <c>null</c>, если TCS не был привязан (Auto-тик — никто не ждёт исход конкретного цикла).
	/// Вызывается в completion-handler'е <c>StageCompleted/Failed</c> для разрешения task'а caller'а.
	/// </summary>
	public TaskCompletionSource? TakeIterationOutcomeTcs() =>
		Interlocked.Exchange(ref _iterationOutcomeTcs, null);

	/// <summary>
	/// Subscribe на поток итераций инстанса. Subscriber — <see cref="IAsyncDisposable"/>; caller использует
	/// <c>await using</c> для отписки. Bulk-finish со стороны owner'а — через <see cref="CompleteIterationSubscribers"/>
	/// (cascade-removal / shutdown). Реализация <see cref="IInstanceHandle.GetAsyncEnumerator"/>.
	/// </summary>
	public BoundedSubscriber<IIterationHandle> SubscribeIterations() {
		var sub = new BoundedSubscriber<IIterationHandle>(IterationBufferCapacity, _iterationSubscribers);
		_iterationSubscribers.TryAdd(sub, 0);
		return sub;
	}

	/// <summary>
	/// Complete все subscriber-каналы — вызывается из event-loop при finalize этого инстанса
	/// (cascade-removal либо shutdown). Consumer'ы получат естественный exit из <c>ReadAllAsync</c>.
	/// </summary>
	public void CompleteIterationSubscribers() {
		foreach (var sub in _iterationSubscribers.Keys) sub.Complete();
		_iterationSubscribers.Clear();
	}

	/// <summary>
	/// Все ключи, опубликованные этим инстансом-эмитером. Read-only-проекция приватного <c>_emittedKeys</c>.
	/// Читается <see cref="EventLoop"/>-ом из своего же потока (через <c>InstanceMatching.ComputeDimension</c>) —
	/// safe-snapshot не нужен.
	/// </summary>
	public IReadOnlyCollection<string> EmittedKeys => _emittedKeys;

	/// <summary>Добавляет ключ в keyspace эмитера. <c>true</c>, если ключ был новый; <c>false</c> — идемпотентно.</summary>
	public bool AddEmittedKey(string key) => _emittedKeys.Add(key);

	/// <summary>Удаляет ключ из keyspace эмитера. <c>true</c>, если ключ существовал.</summary>
	public bool RemoveEmittedKey(string key) => _emittedKeys.Remove(key);

	/// <summary>True, если ключ был ранее эмитирован этим инстансом и ещё не удалён.</summary>
	public bool ContainsEmittedKey(string key) => _emittedKeys.Contains(key);

	/// <summary>
	/// Снимает весь keyspace-bucket (вызывается при cascade-removal инстанса). Возвращает orphan-ключи
	/// для рекурсивного cascade потомков. После вызова <see cref="EmittedKeys"/> пуст.
	/// </summary>
	public IReadOnlyCollection<string> TakeEmittedKeys() {
		if (_emittedKeys.Count == 0) return [];
		var orphans = new List<string>(_emittedKeys);
		_emittedKeys.Clear();
		return orphans;
	}

	/// <summary>Проецирует текущее состояние инстанса в публичный snapshot. Вызывается из <see cref="JobOrchestratorRuntime"/> и InstanceHandle.</summary>
	public InstanceInfo ToInstanceInfo() {
		var stats = Metrics.Stats;
		return new InstanceInfo {
			StageName = Identity.Stage.Name,
			Keys = Identity.Keys,
			FullyQualifiedName = Identity.FullyQualifiedName,
			State = State,
			LastSuccess = stats.LastSuccess,
			LastAttempt = stats.LastAttempt,
			ConsecutiveFailures = stats.ConsecutiveFailures,
			LastError = stats.LastError,
		};
	}
}
