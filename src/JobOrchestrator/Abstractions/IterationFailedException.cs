namespace JobOrchestrator.Abstractions;

/// <summary>
/// Бросается из <see cref="IIterationHandle.Completion"/>, когда итерация завершилась не-успехом.
/// Точная причина — в <see cref="Reason"/>; <see cref="FullyQualifiedName"/> идентифицирует инстанс.
/// Для <see cref="IterationFailureReason.StageException"/> — <see cref="System.Exception.InnerException"/>
/// несёт исходное исключение stage handler'а.
/// <para>
/// Симметрично <see cref="IterationRejectedException"/>: тот покрывает фазу acceptance (до получения
/// handle), этот — фазу execution (после получения handle).
/// </para>
/// </summary>
public sealed class IterationFailedException : Exception {
	public IterationFailureReason Reason { get; }
	public string FullyQualifiedName { get; }

	public IterationFailedException(IterationFailureReason reason, string fullyQualifiedName, string message)
		: base(message) {
		Reason = reason;
		FullyQualifiedName = fullyQualifiedName;
	}

	public IterationFailedException(IterationFailureReason reason, string fullyQualifiedName, string message, Exception innerException)
		: base(message, innerException) {
		Reason = reason;
		FullyQualifiedName = fullyQualifiedName;
	}
}
