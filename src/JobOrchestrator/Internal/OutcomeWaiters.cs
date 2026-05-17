using System.Collections.Concurrent;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForOutcomeAsync"/>: один
/// <see cref="TaskCompletionSource{TResult}"/> на <see cref="InstanceIdentity"/>. Lock-free через
/// <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// <para>
/// В отличие от <see cref="SuccessWaiters"/>, резолвится на ЛЮБОЙ первый исход — Success/Failure/Cancelled.
/// Каждый последующий <see cref="Signal"/> на ту же identity <b>заменяет</b> completed-slot на свежий
/// completed-TCS с новым outcome — семантика «последний outcome выигрывает».
/// </para>
/// <para>
/// <b>Cold Signal мемоизируется.</b> Если на момент <see cref="Signal"/> ещё нет slot'а, создаётся свежий
/// completed-slot с этим outcome. Поздний <see cref="Register"/> получит его завершённую <c>Task</c>
/// немедленно. Симметрично <see cref="SuccessWaiters"/>: реестр помнит последний исход до
/// <see cref="Reset"/> или <see cref="FailAll"/>.
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
	/// Сигнализирует исход. Если slot pending — резолвит. Если slot уже completed — CAS-replace на свежий
	/// completed (последний outcome выигрывает). Если slot отсутствует — атомарно создаёт свежий completed
	/// slot, чтобы поздний <see cref="Register"/> получил исход немедленно.
	/// </summary>
	public void Signal(InstanceIdentity identity, StageOutcome outcome) {
		while (true) {
			// Re-check внутри цикла: FailAll, прилетевший между retry-итерациями, не должен дать нам
			// зарегистрировать slot в уже-остановленном реестре (FailAll сделал _slots.Clear()).
			if (_stopped) return;
			if (_slots.TryGetValue(identity, out var existing)) {
				if (existing.TrySetResult(outcome)) return;
				// Slot уже completed — заменяем на свежий, чтобы поздние Register видели актуальный outcome.
				var fresh = CompletedTcs(outcome);
				if (_slots.TryUpdate(identity, fresh, existing)) return;
				continue; // CAS-loser: другой Signal/Reset изменил slot — retry.
			}
			// Cold Signal — создаём completed-slot для будущих late-register'ов (мемоизация).
			var cold = CompletedTcs(outcome);
			if (_slots.TryAdd(identity, cold)) return;
			// CAS-loser: между TryGetValue и TryAdd кто-то Register-нул — retry основной ветки.
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
