namespace JobOrchestrator.Configuration;

/// <summary>
/// Дефолтные настройки, применяемые к стадиям в момент их объявления через <c>JobOrchestratorBuilder.Stage(...)</c>.
/// Отдельные стадии могут перекрыть значения через <c>Debounce(...)</c>, <c>RetryAfterFailure(...)</c>, <c>WithExecutionTimeout(...)</c>.
/// </summary>
/// <remarks>
/// Snapshot захватывается в момент вызова <c>Stage(name)</c> — последующие изменения <see cref="JobDefaults"/>
/// не влияют на уже созданные стадии.
/// </remarks>
public sealed class JobDefaults {
	/// <summary>Окно дебаунса для Manual-триггера (от <c>LastAttempt</c>, вне зависимости от исхода).</summary>
	public TimeSpan Debounce { get; set; } = TimeSpan.Zero;

	/// <summary>Политика retry после неуспеха итерации.</summary>
	public RetryPolicy RetryAfterFailure { get; set; } = RetryPolicy.NoRetry;

	/// <summary>Watchdog-таймаут одной итерации. <c>null</c> — без таймаута.</summary>
	public TimeSpan? ExecutionTimeout { get; set; }
}
