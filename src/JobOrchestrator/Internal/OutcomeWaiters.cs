using System.Collections.Concurrent;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForOutcomeAsync"/>: один
/// <see cref="TaskCompletionSource{TResult}"/> на <see cref="InstanceIdentity"/>. Lock-free через
/// <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// <para>
/// В отличие от <see cref="SuccessWaiters"/>, резолвится на ЛЮБОЙ первый исход — Success/Failure/Cancelled.
/// Каждый последующий <see cref="Signal"/> на ту же identity <b>заменяет</b> completed-slot на свежий
/// completed-TCS с новым outcome — соответствует семантике «LastOutcome выигрывает» прежнего реестра.
/// </para>
/// <para>
/// <b>Cold Signal — no-op.</b> Если на момент Signal нет slot'а (никто не Register-ил), Signal ничего не
/// создаёт. Поздний Register после такого Signal'а будет ждать СЛЕДУЮЩИЙ Signal. Это намеренное
/// поведение реестра (см. тест <c>Signal_WithoutSubscribers_DoesNotCreateBucket</c>).
/// </para>
/// </summary>
internal sealed class OutcomeWaiters {
	private readonly ConcurrentDictionary<InstanceIdentity, TaskCompletionSource<StageOutcome>> _slots = new();
	private volatile bool _stopped;
	private Exception? _stopReason;

	public Task<StageOutcome> Register(InstanceIdentity identity, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		if (_stopped) return Task.FromException<StageOutcome>(_stopReason!);

		var tcs = _slots.GetOrAdd(identity, _ => new TaskCompletionSource<StageOutcome>(TaskCreationOptions.RunContinuationsAsynchronously));
		// Re-check после GetOrAdd — закрывает race с FailAll (см. SuccessWaiters.Register).
		if (_stopped) tcs.TrySetException(_stopReason!);

		return tcs.Task.WaitAsync(ct);
	}

	/// <summary>
	/// Сигнализирует исход. Если slot отсутствует — no-op (cold Signal не накапливается). Если slot
	/// pending — резолвит. Если slot уже completed — CAS-replace на свежий completed (последний выигрывает).
	/// </summary>
	public void Signal(InstanceIdentity identity, StageOutcome outcome) {
		if (_stopped) return;
		while (true) {
			if (!_slots.TryGetValue(identity, out var existing)) return;
			if (existing.TrySetResult(outcome)) return;
			// existing уже completed — пробуем заменить, чтобы поздние Register видели АКТУАЛЬНЫЙ outcome.
			var fresh = CompletedTcs(outcome);
			if (_slots.TryUpdate(identity, fresh, existing)) return;
			// CAS-loser: другой Signal или Reset изменил slot — retry.
		}
	}

	/// <summary>
	/// Очищает slot для <paramref name="identity"/>. Используется в <c>FinalizeTerminating</c> —
	/// новый инстанс с теми же ключами не должен видеть исход предыдущего.
	/// </summary>
	public void Reset(InstanceIdentity identity) => _slots.TryRemove(identity, out _);

	/// <summary>Завершает все pending исключением и блокирует будущие Register-ы.</summary>
	public void FailAll(Exception ex) {
		_stopReason = ex;
		_stopped = true;
		foreach (var kv in _slots) {
			kv.Value.TrySetException(ex);
		}
		_slots.Clear();
	}

	private static TaskCompletionSource<StageOutcome> CompletedTcs(StageOutcome outcome) {
		var tcs = new TaskCompletionSource<StageOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
		tcs.TrySetResult(outcome);
		return tcs;
	}
}
