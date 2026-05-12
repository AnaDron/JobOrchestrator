namespace JobOrchestrator.Internal;

/// <summary>
/// Чистая функция принятия триггера (Auto или Manual). Применяет retry-delay и debounce-окно
/// по правилам:
/// <list type="bullet">
/// <item>Running → <see cref="TriggerResult.AlreadyRunning"/> для любого источника.</item>
/// <item>Auto в retry-delay (после неуспеха) → <see cref="TriggerResult.WaitingRetry"/>.</item>
/// <item>Manual в окне debounce от <c>LastAttempt</c> (вне зависимости от исхода) → <see cref="TriggerResult.Debounced"/>.</item>
/// <item>Manual игнорирует retry-delay (пользователь явно просит).</item>
/// <item>Auto не проверяет debounce (scheduled tick через interval — анти-spam-click не нужен).</item>
/// </list>
/// </summary>
internal static class TriggerAcceptance {
	public static TriggerResult TryAccept(
		Job job,
		TriggerSource source,
		DateTimeOffset now
	) {
		if (job.State == JobLifecycleState.Running) {
			return TriggerResult.AlreadyRunning;
		}

		if (source == TriggerSource.Auto) {
			if (job.ConsecutiveFailures > 0 && job.LastAttempt.HasValue) {
				var retryDelay = job.Stage.RetryPolicy.ComputeDelay(job.ConsecutiveFailures);
				if (now - job.LastAttempt.Value < retryDelay) {
					return TriggerResult.WaitingRetry;
				}
			}
			return TriggerResult.Started;
		}

		// source == Manual
		if (job.LastAttempt.HasValue && (now - job.LastAttempt.Value) < job.Stage.Debounce) {
			return TriggerResult.Debounced;
		}
		return TriggerResult.Started;
	}
}
