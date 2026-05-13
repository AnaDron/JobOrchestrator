namespace JobOrchestrator.Internal;

/// <summary>
/// Runtime-сущность одного инстанса стадии (long-lived). Несколько <see cref="StageInstance"/> могут
/// разделять один <see cref="StageDescriptor"/> — один на каждый компонент композитного ключа.
/// <para>
/// Мутирующие поля (<c>_lastSuccessTicks</c>, <c>_lastAttemptTicks</c>, <c>_nextAutoTicks</c>,
/// <c>_consecutiveFailures</c>, <c>_lastError</c>, <c>_state</c>) изменяются единственным writer-потоком
/// (event-loop consumer). Reader-потоки (DueScanner, GetOverview из внешнего кода) читают значения
/// через <see cref="Volatile.Read{T}(ref T)"/> — атомарно, без блокировок, без аллокаций.
/// </para>
/// <para>
/// Для <c>DateTimeOffset?</c> хранится <c>long</c>-encoded UtcTicks (0 = null), что даёт word-aligned
/// atomic read/write на 64-bit платформах. Это устраняет risk torn-read многобайтных <c>DateTimeOffset</c>
/// при concurrent чтении из внешних потоков, без оверхеда immutable-record allocation на каждое обновление.
/// </para>
/// </summary>
internal sealed class StageInstance {
	public required StageDescriptor Stage { get; init; }
	public required IReadOnlyDictionary<string, string> DependencyKeys { get; init; }
	public required string FullyQualifiedName { get; init; }
	public required string EncodedKey { get; init; }

	/// <summary>Scope для <see cref="IJobStateStore"/>: <c>"{StageName}:{EncodedKey}"</c>.</summary>
	public string StateScope => $"{Stage.Name}:{EncodedKey}";

	long _lastSuccessTicks;
	long _lastAttemptTicks;
	long _nextAutoTicks;
	int _consecutiveFailures;
	string? _lastError;
	int _state; // 0 = Idle, 1 = Running

	/// <summary>Текущее состояние lifecycle.</summary>
	public InstanceLifecycleState State {
		get => Volatile.Read(ref _state) == 0 ? InstanceLifecycleState.Idle : InstanceLifecycleState.Running;
		set => Volatile.Write(ref _state, value == InstanceLifecycleState.Idle ? 0 : 1);
	}

	/// <summary>
	/// Время последнего успешного завершения. Монотонно: после первого != null значение никогда не возвращается к null.
	/// Atomic read/write через long-encoded UtcTicks.
	/// </summary>
	public DateTimeOffset? LastSuccess {
		get {
			long t = Volatile.Read(ref _lastSuccessTicks);
			return t == 0 ? null : new DateTimeOffset(t, TimeSpan.Zero);
		}
		set => Volatile.Write(ref _lastSuccessTicks, value?.UtcTicks ?? 0);
	}

	/// <summary>Время последней попытки (успешной или неуспешной).</summary>
	public DateTimeOffset? LastAttempt {
		get {
			long t = Volatile.Read(ref _lastAttemptTicks);
			return t == 0 ? null : new DateTimeOffset(t, TimeSpan.Zero);
		}
		set => Volatile.Write(ref _lastAttemptTicks, value?.UtcTicks ?? 0);
	}

	/// <summary>
	/// Время следующего Auto-тика. <c>null</c> = инстанс не запланирован (только что создан или Running).
	/// Используется <c>DueScanner</c> для вычисления ближайшего due-времени.
	/// </summary>
	public DateTimeOffset? NextAutoUtc {
		get {
			long t = Volatile.Read(ref _nextAutoTicks);
			return t == 0 ? null : new DateTimeOffset(t, TimeSpan.Zero);
		}
		set => Volatile.Write(ref _nextAutoTicks, value?.UtcTicks ?? 0);
	}

	/// <summary>Серия последовательных неуспехов с момента последнего успеха.</summary>
	public int ConsecutiveFailures {
		get => Volatile.Read(ref _consecutiveFailures);
		set => Volatile.Write(ref _consecutiveFailures, value);
	}

	/// <summary>Сообщение последней ошибки или <c>null</c>.</summary>
	public string? LastError {
		get => Volatile.Read(ref _lastError);
		set => Volatile.Write(ref _lastError, value);
	}

	/// <summary>CTS текущей итерации (если State == Running), иначе null.</summary>
	public CancellationTokenSource? RunCts { get; set; }

	/// <summary>Task текущей итерации (если State == Running). Используется для graceful-await при cascade-удалении.</summary>
	public Task? RunningTask { get; set; }
}
