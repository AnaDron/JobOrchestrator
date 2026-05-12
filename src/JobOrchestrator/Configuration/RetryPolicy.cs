namespace JobOrchestrator.Configuration;

/// <summary>Политика расчёта задержки перед следующей попыткой после неуспеха.</summary>
/// <remarks>
/// Используется для расчёта <c>nextTickAt = LastAttempt + ComputeDelay(ConsecutiveFailures)</c> после провала итерации.
/// Auto-триггер уважает retry-delay (отказывает с <see cref="TriggerResult.WaitingRetry"/>); Manual игнорирует.
/// </remarks>
public abstract record RetryPolicy {
	/// <summary>Задержка перед следующей попыткой при указанном числе последовательных неуспехов.</summary>
	public abstract TimeSpan ComputeDelay(int consecutiveFailures);

	/// <summary>Политика «не повторять» — после неуспеха стадия больше не тикает по Auto до следующего нормального интервала.</summary>
	public static RetryPolicy NoRetry { get; } = new NoRetryPolicy();

	/// <summary>Фиксированная задержка между попытками.</summary>
	public static RetryPolicy FixedDelay(TimeSpan delay) {
		if (delay < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(delay), "Задержка не может быть отрицательной.");
		return new FixedDelayPolicy(delay);
	}

	/// <summary>Экспоненциальный backoff: <c>initial * 2^(n-1)</c>, capped <paramref name="max"/>.</summary>
	public static RetryPolicy ExponentialBackoff(TimeSpan initial, TimeSpan max) {
		if (initial <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(initial), "Начальная задержка должна быть положительной.");
		if (max < initial) throw new ArgumentOutOfRangeException(nameof(max), "Максимальная задержка не может быть меньше начальной.");
		return new ExponentialBackoffPolicy(initial, max);
	}

	private sealed record NoRetryPolicy : RetryPolicy {
		public override TimeSpan ComputeDelay(int consecutiveFailures) => TimeSpan.Zero;
	}

	private sealed record FixedDelayPolicy(TimeSpan Delay) : RetryPolicy {
		public override TimeSpan ComputeDelay(int consecutiveFailures) => consecutiveFailures > 0 ? Delay : TimeSpan.Zero;
	}

	private sealed record ExponentialBackoffPolicy(TimeSpan Initial, TimeSpan Max) : RetryPolicy {
		public override TimeSpan ComputeDelay(int consecutiveFailures) {
			if (consecutiveFailures <= 0) return TimeSpan.Zero;
			double doubled = Initial.TotalMilliseconds * Math.Pow(2, consecutiveFailures - 1);
			double capped = Math.Min(doubled, Max.TotalMilliseconds);
			return TimeSpan.FromMilliseconds(capped);
		}
	}
}
