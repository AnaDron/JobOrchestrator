namespace JobOrchestrator.Tests.Helpers;

/// <summary>
/// Fluent-extensions для setup-а <see cref="Instance"/> в unit-тестах.
/// </summary>
internal static class InstanceTestExtensions {
	public static Instance WithLastSuccess(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics.WithLastSuccess(at));
		return instance;
	}

	public static Instance WithLastAttempt(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics.WithLastAttempt(at));
		return instance;
	}

	public static Instance WithFailures(this Instance instance, int count) {
		instance.SetMetrics(instance.Metrics.WithConsecutiveFailures(count));
		return instance;
	}

	public static Instance WithLastError(this Instance instance, string? error) {
		instance.SetMetrics(instance.Metrics.WithLastError(error));
		return instance;
	}

	public static Instance WithNextAuto(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics.WithNextAutoUtc(at));
		return instance;
	}
}
