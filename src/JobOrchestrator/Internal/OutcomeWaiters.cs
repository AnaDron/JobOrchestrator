using System.Collections.Concurrent;
using JobOrchestrator.Abstractions;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForOutcomeAsync"/>: один bucket на
/// <see cref="InstanceIdentity"/>. Семантика «следующий исход после регистрации» через
/// monotonic <see cref="WaiterBucket.CompletedGeneration"/>.
/// <para>
/// В отличие от <see cref="SuccessWaiters"/>, прошлый исход не мемоизируется для новых
/// Register-ов: каждый вызов ждёт <c>CompletedGeneration + 1</c>.
/// </para>
/// </summary>
internal sealed class OutcomeWaiters {
	private readonly ConcurrentDictionary<InstanceIdentity, WaiterBucket> _buckets = new();
	private volatile bool _stopped;
	private Exception? _stopReason;

	public Task<StageOutcome> Register(InstanceIdentity identity, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		if (_stopped) return Task.FromException<StageOutcome>(_stopReason!);

		var bucket = _buckets.GetOrAdd(identity, static _ => new WaiterBucket());
		if (_stopped) return Task.FromException<StageOutcome>(_stopReason!);

		var tcs = new TaskCompletionSource<StageOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
		lock (bucket.Lock) {
			int targetGeneration = bucket.CompletedGeneration + 1;
			if (bucket.CompletedGeneration >= targetGeneration && bucket.LastOutcome is { } ready) {
				tcs.TrySetResult(ready);
			} else {
				bucket.Pending.Add((targetGeneration, tcs));
				// Signal мог прийти с другого потока между первой проверкой и Add.
				if (bucket.CompletedGeneration >= targetGeneration && bucket.LastOutcome is { } late) {
					bucket.Pending.RemoveAll(p => ReferenceEquals(p.Tcs, tcs));
					tcs.TrySetResult(late);
				}
			}
		}

		if (_stopped) {
			lock (bucket.Lock) bucket.Pending.RemoveAll(p => ReferenceEquals(p.Tcs, tcs));
			tcs.TrySetException(_stopReason!);
		}

		return tcs.Task.WaitAsync(ct);
	}

	public void Signal(InstanceIdentity identity, StageOutcome outcome) {
		if (_stopped) return;

		var bucket = _buckets.GetOrAdd(identity, static _ => new WaiterBucket());
		TaskCompletionSource<StageOutcome>[] toResolve;
		lock (bucket.Lock) {
			bucket.CompletedGeneration++;
			bucket.LastOutcome = outcome;
			int generation = bucket.CompletedGeneration;
			var pending = bucket.Pending;
			var resolved = new List<TaskCompletionSource<StageOutcome>>();
			for (int i = pending.Count - 1; i >= 0; i--) {
				var (targetGen, waiterTcs) = pending[i];
				if (targetGen > generation) continue;
				resolved.Add(waiterTcs);
				pending.RemoveAt(i);
			}
			toResolve = resolved.ToArray();
		}
		foreach (var t in toResolve) t.TrySetResult(outcome);
	}

	/// <summary>
	/// Сбрасывает bucket при удалении инстанса. Pending должны быть уже резолвлены через
	/// <see cref="Signal"/>-Cancelled до Reset.
	/// </summary>
	public void Reset(InstanceIdentity identity) {
		if (!_buckets.TryRemove(identity, out var bucket)) return;

		TaskCompletionSource<StageOutcome>[] stranded;
		lock (bucket.Lock) {
			stranded = bucket.Pending.ConvertAll(static p => p.Tcs).ToArray();
			bucket.Pending.Clear();
		}
		var fallback = StageOutcome.FromCancellation(
			new InvalidOperationException("Инстанс удалён до получения исхода."));
		foreach (var t in stranded) t.TrySetResult(fallback);
	}

	public void FailAll(Exception ex) {
		_stopReason = ex;
		_stopped = true;
		foreach (var kv in _buckets) {
			TaskCompletionSource<StageOutcome>[] pending;
			lock (kv.Value.Lock) {
				pending = kv.Value.Pending.ConvertAll(static p => p.Tcs).ToArray();
				kv.Value.Pending.Clear();
			}
			foreach (var t in pending) t.TrySetException(ex);
		}
		_buckets.Clear();
	}

	sealed class WaiterBucket {
		public object Lock { get; } = new();
		public int CompletedGeneration { get; set; }
		public StageOutcome? LastOutcome { get; set; }
		public List<(int TargetGeneration, TaskCompletionSource<StageOutcome> Tcs)> Pending { get; } = [];
	}
}
