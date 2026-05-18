namespace JobOrchestrator.Internal;

/// <summary>Результат политики <see cref="TriggerAcceptance.TryAccept"/> — внутренний gate event loop.</summary>
internal readonly struct TriggerAcceptanceDecision {
	public TriggerResult? Rejection { get; init; }

	public bool IsAccepted => Rejection is null;

	public static TriggerAcceptanceDecision Accept => default;

	public static TriggerAcceptanceDecision Reject(TriggerResult result) => new() { Rejection = result };
}
