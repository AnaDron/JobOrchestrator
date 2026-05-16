namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForOutcomeAsync"/>: одна группа waiter-ов на
/// <see cref="InstanceIdentity"/>. В отличие от <see cref="SuccessWaiters"/>, резолвится на ЛЮБОЙ
/// первый исход стадии — Success/Failure/Cancelled.
/// <para>
/// Bucket хранит <c>LastOutcome</c>: late-register, прибежавший после сигнала, получает последний
/// исход сразу. <see cref="Reset"/> очищает bucket — новые регистрации создают чистый bucket
/// (используется при <c>FinalizeTerminating</c>, чтобы новый инстанс с теми же ключами не наследовал
/// чужой outcome).
/// </para>
/// </summary>
internal sealed class OutcomeWaiters {
	private readonly object _gate = new();
	private readonly Dictionary<InstanceIdentity, WaiterBucket> _buckets = new();
	private bool _stopped;
	private Exception? _stopReason;

	public Task<StageOutcome> Register(InstanceIdentity identity, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		var tcs = new TaskCompletionSource<StageOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);

		WaiterBucket bucket;
		lock (_gate) {
			if (_stopped) {
				tcs.TrySetException(_stopReason!);
				return tcs.Task;
			}
			if (!_buckets.TryGetValue(identity, out var existing)) {
				existing = new WaiterBucket();
				_buckets[identity] = existing;
			}
			bucket = existing;
		}

		bool added = false;
		lock (bucket.Lock) {
			if (bucket.LastOutcome is { } last) {
				tcs.TrySetResult(last);
			} else {
				bucket.Pending.Add(tcs);
				added = true;
			}
		}

		if (added && ct.CanBeCanceled) {
			var reg = ct.Register(state => {
				var t = (TaskCompletionSource<StageOutcome>)state!;
				if (t.TrySetCanceled()) {
					lock (bucket.Lock) bucket.Pending.Remove(t);
				}
			}, tcs);
			tcs.Task.ContinueWith(static (_, r) => ((CancellationTokenRegistration)r!).Dispose(),
				reg, TaskScheduler.Default);
		}

		return tcs.Task;
	}

	/// <summary>Сигнализирует исход (Success/Failure/Cancelled), резолвит всех pending и запоминает.</summary>
	public void Signal(InstanceIdentity identity, StageOutcome outcome) {
		WaiterBucket bucket;
		lock (_gate) {
			if (!_buckets.TryGetValue(identity, out var existing)) {
				existing = new WaiterBucket();
				_buckets[identity] = existing;
			}
			bucket = existing;
		}
		TaskCompletionSource<StageOutcome>[] toResolve;
		lock (bucket.Lock) {
			bucket.LastOutcome = outcome;
			toResolve = [.. bucket.Pending];
			bucket.Pending.Clear();
		}
		foreach (var t in toResolve) t.TrySetResult(outcome);
	}

	/// <summary>
	/// Очищает bucket для <paramref name="identity"/>. Используется в <c>FinalizeTerminating</c> —
	/// новый инстанс с теми же ключами не должен видеть исход предыдущего.
	/// </summary>
	public void Reset(InstanceIdentity identity) {
		lock (_gate) _buckets.Remove(identity);
	}

	/// <summary>Завершает все pending исключением и блокирует будущие Register-ы.</summary>
	public void FailAll(Exception ex) {
		WaiterBucket[] all;
		lock (_gate) {
			_stopped = true;
			_stopReason = ex;
			all = [.. _buckets.Values];
			_buckets.Clear();
		}
		foreach (var bucket in all) {
			TaskCompletionSource<StageOutcome>[] toResolve;
			lock (bucket.Lock) {
				toResolve = [.. bucket.Pending];
				bucket.Pending.Clear();
			}
			foreach (var t in toResolve) t.TrySetException(ex);
		}
	}

	private sealed class WaiterBucket {
		public object Lock { get; } = new();
		public List<TaskCompletionSource<StageOutcome>> Pending { get; } = [];
		public StageOutcome? LastOutcome { get; set; }
	}
}
