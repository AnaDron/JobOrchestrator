namespace JobOrchestrator.Internal;

/// <summary>
/// Чистая функция принятия триггера (Auto или Manual). Применяет retry-delay и debounce-окно
/// по правилам:
/// <list type="bullet">
/// <item>Running → <see cref="TriggerResult.AlreadyRunning"/> для любого источника.</item>
/// <item>Terminating → <see cref="TriggerResult.Terminating"/> (инстанс в каскадном удалении).</item>
/// <item>Auto в retry-delay (после неуспеха) → <see cref="TriggerResult.WaitingRetry"/>.</item>
/// <item>Manual в окне debounce от <c>LastAttempt</c> (вне зависимости от исхода) → <see cref="TriggerResult.Debounced"/>.</item>
/// <item>Manual игнорирует retry-delay (пользователь явно просит).</item>
/// <item>Auto не проверяет debounce (scheduled tick через interval — анти-spam-click не нужен).</item>
/// </list>
/// </summary>
internal static class TriggerAcceptance {
	public static TriggerAcceptanceDecision TryAccept(
		Instance instance,
		TriggerSource source,
		DateTimeOffset now
	) {
		var state = instance.State;
		if (state == InstanceLifecycleState.Terminating) return TriggerAcceptanceDecision.Reject(TriggerResult.Terminating);
		if (state == InstanceLifecycleState.Running) return TriggerAcceptanceDecision.Reject(TriggerResult.AlreadyRunning);

		var stats = instance.Metrics.Stats;

		if (source == TriggerSource.Auto) {
			if (stats.ConsecutiveFailures > 0 && stats.LastAttempt.HasValue) {
				var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(stats.ConsecutiveFailures);
				if (now - stats.LastAttempt.Value < retryDelay) {
					return TriggerAcceptanceDecision.Reject(TriggerResult.WaitingRetry);
				}
			}
			return TriggerAcceptanceDecision.Accept;
		}

		if (stats.LastAttempt.HasValue && (now - stats.LastAttempt.Value) < instance.Stage.Debounce) {
			return TriggerAcceptanceDecision.Reject(TriggerResult.Debounced);
		}
		return TriggerAcceptanceDecision.Accept;
	}
}
