namespace JobOrchestrator.Tests.Helpers;

/// <summary>
/// Test-only extension'ы для точечного обновления <see cref="InstanceExecutionStats"/>-полей через
/// <see cref="JobMetrics"/>. В production-коде writers обновляют несколько полей сразу через единый
/// <c>new JobMetrics(...)</c> либо вложенные <c>with</c>-блоки (atomic-snapshot), поэтому точечные
/// helper'ы там не нужны.
/// </summary>
internal static class JobMetricsTestExtensions {
	public static JobMetrics WithLastSuccess(this JobMetrics metrics, DateTimeOffset? lastSuccess) =>
		metrics with { Stats = metrics.Stats with { LastSuccess = lastSuccess } };

	public static JobMetrics WithLastAttempt(this JobMetrics metrics, DateTimeOffset? lastAttempt) =>
		metrics with { Stats = metrics.Stats with { LastAttempt = lastAttempt } };

	public static JobMetrics WithConsecutiveFailures(this JobMetrics metrics, int consecutiveFailures) =>
		metrics with { Stats = metrics.Stats with { ConsecutiveFailures = consecutiveFailures } };

	public static JobMetrics WithLastError(this JobMetrics metrics, string? lastError) =>
		metrics with { Stats = metrics.Stats with { LastError = lastError } };
}
