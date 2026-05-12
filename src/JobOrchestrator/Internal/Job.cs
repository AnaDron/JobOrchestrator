namespace JobOrchestrator.Internal;

/// <summary>
/// Runtime-сущность одного инстанса. Все mutable-поля изменяются единственным consumer-потоком event loop'а —
/// поэтому без блокировок и без CAS-операций.
/// </summary>
internal sealed class Job {
	public required StageDescriptor Stage { get; init; }
	public required IReadOnlyDictionary<string, string> DependencyKeys { get; init; }
	public required string FullyQualifiedName { get; init; }
	public required string EncodedKey { get; init; }

	public JobLifecycleState State { get; set; } = JobLifecycleState.Idle;

	/// <summary>Время последнего успешного завершения. Монотонно: после первого != null значение никогда не возвращается к null.</summary>
	public DateTimeOffset? LastSuccess { get; set; }

	/// <summary>Время последней попытки (успешной или неуспешной).</summary>
	public DateTimeOffset? LastAttempt { get; set; }

	/// <summary>Серия последовательных неуспехов с момента последнего успеха.</summary>
	public int ConsecutiveFailures { get; set; }

	/// <summary>Сообщение последней ошибки или <c>null</c>.</summary>
	public string? LastError { get; set; }

	/// <summary>Время следующего Auto-тика по <c>Environment.TickCount64</c>.</summary>
	public long NextTickAtMs { get; set; }

	/// <summary>CTS текущей итерации (если State == Running), иначе null.</summary>
	public CancellationTokenSource? RunCts { get; set; }
}
