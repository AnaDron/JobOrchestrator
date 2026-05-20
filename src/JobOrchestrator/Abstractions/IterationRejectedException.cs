namespace JobOrchestrator.Abstractions;

/// <summary>
/// Бросается <see cref="IInstanceHandle.RunAsync"/>, если итерация не была запущена.
/// Точная причина — в <see cref="Reason"/>; <see cref="FullyQualifiedName"/> идентифицирует инстанс,
/// для которого попытка была сделана. Caller pattern-match'ит по <c>Reason</c> для разделения
/// retry-able (<c>ConcurrencyDeferred</c>, <c>Debounced</c>) от terminal (<c>NotFound</c>, <c>Faulted</c>).
/// </summary>
public sealed class IterationRejectedException : Exception {
	public IterationRejectReason Reason { get; }
	public string FullyQualifiedName { get; }

	public IterationRejectedException(IterationRejectReason reason, string fullyQualifiedName, string message)
		: base(message) {
		Reason = reason;
		FullyQualifiedName = fullyQualifiedName;
	}

	/// <summary>
	/// Wrap-конструктор: исходная причина (shutdown / handler crash) попадает в
	/// <see cref="Exception.InnerException"/>, чтобы caller сохранил полный stack trace.
	/// </summary>
	public IterationRejectedException(IterationRejectReason reason, string fullyQualifiedName, string message, Exception innerException)
		: base(message, innerException) {
		Reason = reason;
		FullyQualifiedName = fullyQualifiedName;
	}
}
