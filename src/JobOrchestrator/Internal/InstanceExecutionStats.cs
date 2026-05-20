namespace JobOrchestrator.Internal;

/// <summary>
/// Диагностический snapshot исполнения инстанса: история успехов/неуспехов.
/// Часть атомарного <see cref="JobMetrics"/>-снимка.
/// </summary>
/// <param name="LastSuccess">
/// Время последнего успешного завершения. Монотонно: после первого != null значение никогда
/// не возвращается к null (writer event-loop'а не сбрасывает его на последующих неуспехах).
/// </param>
/// <param name="LastAttempt">Время последней попытки (успешной или неуспешной).</param>
/// <param name="ConsecutiveFailures">Серия последовательных неуспехов с момента последнего успеха.</param>
/// <param name="LastError">Сообщение последней ошибки или <c>null</c>.</param>
internal sealed record InstanceExecutionStats(
	DateTimeOffset? LastSuccess,
	DateTimeOffset? LastAttempt,
	int ConsecutiveFailures,
	string? LastError
) {
	public static InstanceExecutionStats Empty { get; } = new(null, null, 0, null);
}
