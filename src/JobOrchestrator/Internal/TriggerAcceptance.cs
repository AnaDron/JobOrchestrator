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
		StageInstance instance,
		TriggerSource source,
		DateTimeOffset now
	) {
		if (instance.State == InstanceLifecycleState.Running) {
			return TriggerResult.AlreadyRunning;
		}

		if (source == TriggerSource.Auto) {
			if (instance.ConsecutiveFailures > 0 && instance.LastAttempt.HasValue) {
				var retryDelay = instance.Stage.RetryPolicy.ComputeDelay(instance.ConsecutiveFailures);
				if (now - instance.LastAttempt.Value < retryDelay) {
					return TriggerResult.WaitingRetry;
				}
			}
			return TriggerResult.Started;
		}

		// source == Manual
		if (instance.LastAttempt.HasValue && (now - instance.LastAttempt.Value) < instance.Stage.Debounce) {
			return TriggerResult.Debounced;
		}
		return TriggerResult.Started;
	}
}
