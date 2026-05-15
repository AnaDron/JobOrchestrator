namespace JobOrchestrator.Tests.Helpers;

/// <summary>
/// Fluent-extensions для setup-а <see cref="Instance"/> в unit-тестах. Заменяет verbose
/// <c>SetMetrics(Metrics with { ... })</c>-pattern.
/// <para>
/// Использование:
/// <code>
/// var inst = TestStages.Make("x").CreateInstance().WithLastSuccess(at).WithFailures(3);
/// </code>
/// </para>
/// </summary>
internal static class InstanceTestExtensions {
	public static Instance WithLastSuccess(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics with { LastSuccess = at });
		return instance;
	}

	public static Instance WithLastAttempt(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics with { LastAttempt = at });
		return instance;
	}

	public static Instance WithFailures(this Instance instance, int count) {
		instance.SetMetrics(instance.Metrics with { ConsecutiveFailures = count });
		return instance;
	}

	public static Instance WithLastError(this Instance instance, string? error) {
		instance.SetMetrics(instance.Metrics with { LastError = error });
		return instance;
	}

	public static Instance WithNextAuto(this Instance instance, DateTimeOffset? at) {
		instance.SetMetrics(instance.Metrics with { NextAutoUtc = at });
		return instance;
	}
}
