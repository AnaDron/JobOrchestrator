namespace JobOrchestrator.Abstractions;

/// <summary>Снимок состояния одного инстанса для <see cref="InstancesOverview"/>.</summary>
public sealed record InstanceInfo {
	/// <summary>Имя стадии этого инстанса.</summary>
	public required string StageName { get; init; }

	/// <summary>Композитный ключ инстанса (см. <see cref="JobContext.Keys"/>).</summary>
	public required InstanceKeys Keys { get; init; }

	/// <summary>Имя инстанса для логирования (см. <see cref="JobContext.FullyQualifiedName"/>).</summary>
	public required string FullyQualifiedName { get; init; }

	/// <summary>Текущее состояние.</summary>
	public required InstanceLifecycleState State { get; init; }

	/// <summary>Время последнего успеха или <c>null</c>, если инстанс ещё ни разу не был успешен. Монотонная метка.</summary>
	public DateTimeOffset? LastSuccess { get; init; }

	/// <summary>Время последней попытки (успешной или неуспешной).</summary>
	public DateTimeOffset? LastAttempt { get; init; }

	/// <summary>Число последовательных неуспехов с момента последнего успеха.</summary>
	public int ConsecutiveFailures { get; init; }

	/// <summary>Сообщение последней ошибки или <c>null</c>.</summary>
	public string? LastError { get; init; }
}
