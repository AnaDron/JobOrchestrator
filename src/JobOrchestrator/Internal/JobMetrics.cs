namespace JobOrchestrator.Internal;

/// <summary>
/// Immutable snapshot мутирующихся метрик инстанса. Заменяется атомарно через
/// <see cref="Interlocked.Exchange{T}(ref T, T)"/>. Read-side получает консистентный snapshot
/// всей пятёрки полей (LastSuccess, LastAttempt, ConsecutiveFailures, LastError, NextAutoUtc)
/// из одной «эпохи» writer'а — устраняет torn-read между полями, в отличие от разрозненных
/// volatile-fields где writer мог обновить часть полей между чтениями reader'а.
/// </summary>
/// <param name="LastSuccess">
/// Время последнего успешного завершения. Монотонно: после первого != null значение никогда
/// не возвращается к null (writer event-loop'а не сбрасывает его на последующих неуспехах).
/// </param>
/// <param name="LastAttempt">Время последней попытки (успешной или неуспешной).</param>
/// <param name="ConsecutiveFailures">Серия последовательных неуспехов с момента последнего успеха.</param>
/// <param name="LastError">Сообщение последней ошибки или <c>null</c>.</param>
/// <param name="NextAutoUtc">
/// Время следующего Auto-тика. <c>null</c> = инстанс не запланирован (только что создан или Running —
/// перепланируется в StageCompleted/Failed-handler-е).
/// </param>
internal sealed record JobMetrics(
	DateTimeOffset? LastSuccess,
	DateTimeOffset? LastAttempt,
	int ConsecutiveFailures,
	string? LastError,
	DateTimeOffset? NextAutoUtc
) {
	public static JobMetrics Empty { get; } = new(null, null, 0, null, null);
}
