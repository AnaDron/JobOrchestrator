using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// Manual-методы публикуют события в <c>Channel</c>; GetOverview читает <c>JobManager</c> снапшот.
/// </summary>
internal sealed class JobOrchestratorRuntime(
	Channel<OrchestratorEvent> channel,
	JobManager jobs
) : IJobOrchestrator {
	public async Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		var keys = dependencyKeys ?? new Dictionary<string, string>(StringComparer.Ordinal);
		var tcs = new TaskCompletionSource<TriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(stageName, keys, tcs);
		await channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
	}

	public void RegisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		channel.Writer.TryWrite(new KeyAddedEvent(stageName, key));
	}

	public void UnregisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		channel.Writer.TryWrite(new KeyRemovedEvent(stageName, key));
	}

	public JobOverview GetOverview() => jobs.ToOverview();
}
