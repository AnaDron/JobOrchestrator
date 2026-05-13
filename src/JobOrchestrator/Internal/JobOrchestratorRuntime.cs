using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// Все методы публикуют события в <c>Channel</c>; GetOverview читает снапшот тоже через event loop
/// (для thread-safe доступа без блокировок).
/// </summary>
internal sealed class JobOrchestratorRuntime(
	Channel<OrchestratorEvent> channel,
	OrchestratorLifecycle lifecycle
) : IJobOrchestrator {
	public bool IsFaulted => lifecycle.IsFaulted;

	public async Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (lifecycle.IsFaulted) return TriggerResult.Faulted;

		var keys = dependencyKeys ?? new Dictionary<string, string>(StringComparer.Ordinal);
		var tcs = new TaskCompletionSource<TriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(stageName, keys, tcs);
		try {
			await channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			return TriggerResult.Faulted;
		}
		return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
	}

	public void RegisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		channel.Writer.Publish(new KeyAddedEvent(stageName, key));
	}

	public void UnregisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		channel.Writer.Publish(new KeyRemovedEvent(stageName, key));
	}

	public async Task<InstancesOverview> GetOverviewAsync(CancellationToken ct = default) {
		ThrowIfFaulted();
		var tcs = new TaskCompletionSource<InstancesOverview>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new OverviewRequestedEvent(tcs);
		try {
			await channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			throw new InvalidOperationException("Оркестратор остановлен или находится в Faulted-состоянии.");
		}
		return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
	}

	private void ThrowIfFaulted() {
		if (lifecycle.IsFaulted) {
			throw new InvalidOperationException("Оркестратор находится в Faulted-состоянии — операции недоступны до рестарта.");
		}
	}
}
