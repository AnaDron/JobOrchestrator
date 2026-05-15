namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Polling-primitive с timeout и описательным сообщением для async-условий в integration-тестах.
/// Лучше ad-hoc <c>Task.Delay + WaitForCallCountAsync</c>, потому что работает с любым предикатом
/// и даёт чёткое сообщение об ошибке при timeout — критично для debugging flaky-тестов.
/// </summary>
internal static class AsyncWait {
	private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

	public static Task UntilAsync(Func<bool> condition, TimeSpan timeout, string description, TimeProvider? time = null) =>
		UntilAsync(() => Task.FromResult(condition()), timeout, description, time);

	public static async Task UntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string description, TimeProvider? time = null) {
		var tp = time ?? TimeProvider.System;
		var deadline = tp.GetUtcNow() + timeout;
		while (tp.GetUtcNow() < deadline) {
			if (await condition().ConfigureAwait(false)) return;
			await Task.Delay(PollInterval, tp).ConfigureAwait(false);
		}
		throw new TimeoutException($"Условие '{description}' не выполнилось за {timeout}.");
	}
}
