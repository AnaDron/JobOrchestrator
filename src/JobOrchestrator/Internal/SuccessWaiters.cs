namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IJobOrchestrator.WaitForStageSuccessAsync"/>: одна группа waiter-ов на пару
/// <c>(stageName, encodedKey)</c>. Сигнал об успехе резолвит все накопленные TCS этой группы и запоминает
/// <c>Completed=true</c> — поэтому late-register, прибежавший ПОСЛЕ сигнала, получит уже-завершённый Task.
/// <para>
/// Корректность под race:
/// </para>
/// <list type="number">
/// <item><see cref="SignalSuccess"/> сначала GetOrAdd bucket, потом ставит <c>Completed=true</c> →
///       <see cref="Register"/>, прилетевший ПОСЛЕ сигнала, увидит флаг и резолвится сразу.</item>
/// <item>Флаг <c>_stopped</c> закрывает регистрацию после <see cref="FailAll"/>: новые caller-ы
///       немедленно получают исключение, а не зависают в пустом bucket-е.</item>
/// <item>CancellationToken: register-cleanup снимает TCS из bucket'а при отмене — не накапливаем
///       отменённые ссылки в долгоживущем bucket.</item>
/// </list>
/// </summary>
internal sealed class SuccessWaiters {
	private readonly object _gate = new();
	private readonly Dictionary<(string Stage, string Encoded), WaiterBucket> _buckets = new();
	private bool _stopped;
	private Exception? _stopReason;

	public Task Register(string stage, string encodedKey, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		WaiterBucket bucket;
		lock (_gate) {
			if (_stopped) {
				tcs.TrySetException(_stopReason!);
				return tcs.Task;
			}
			if (!_buckets.TryGetValue((stage, encodedKey), out var existing)) {
				existing = new WaiterBucket();
				_buckets[(stage, encodedKey)] = existing;
			}
			bucket = existing;
		}

		bool added = false;
		lock (bucket.Lock) {
			if (bucket.Completed) {
				tcs.TrySetResult();
			} else {
				bucket.Pending.Add(tcs);
				added = true;
			}
		}

		if (added && ct.CanBeCanceled) {
			var reg = ct.Register(state => {
				var t = (TaskCompletionSource)state!;
				if (t.TrySetCanceled()) {
					lock (bucket.Lock) bucket.Pending.Remove(t);
				}
			}, tcs);
			tcs.Task.ContinueWith(static (_, r) => ((CancellationTokenRegistration)r!).Dispose(),
				reg, TaskScheduler.Default);
		}

		return tcs.Task;
	}

	/// <summary>Сигнализирует первый success — резолвит всех pending и запоминает Completed=true для late-register'ов.</summary>
	public void SignalSuccess(string stage, string encodedKey) {
		WaiterBucket bucket;
		lock (_gate) {
			if (!_buckets.TryGetValue((stage, encodedKey), out var existing)) {
				existing = new WaiterBucket();
				_buckets[(stage, encodedKey)] = existing;
			}
			bucket = existing;
		}
		TaskCompletionSource[] toResolve;
		lock (bucket.Lock) {
			bucket.Completed = true;
			toResolve = [.. bucket.Pending];
			bucket.Pending.Clear();
		}
		foreach (var t in toResolve) t.TrySetResult();
	}

	/// <summary>
	/// Удаляет bucket и завершает все pending исключением. Вызывается, когда инстанс удалён каскадом —
	/// дальнейшее ожидание success бессмысленно. Будущие Register-ы на эту же пару создадут чистый bucket.
	/// </summary>
	public void SignalCancellation(string stage, string encodedKey, Exception ex) {
		WaiterBucket? bucket;
		lock (_gate) {
			if (!_buckets.Remove((stage, encodedKey), out bucket)) return;
		}
		TaskCompletionSource[] toResolve;
		lock (bucket.Lock) {
			toResolve = [.. bucket.Pending];
			bucket.Pending.Clear();
		}
		foreach (var t in toResolve) t.TrySetException(ex);
	}

	/// <summary>
	/// Завершает все pending исключением и блокирует будущие Register-ы. Вызывается при shutdown/fault.
	/// </summary>
	public void FailAll(Exception ex) {
		WaiterBucket[] all;
		lock (_gate) {
			_stopped = true;
			_stopReason = ex;
			all = [.. _buckets.Values];
			_buckets.Clear();
		}
		foreach (var bucket in all) {
			TaskCompletionSource[] toResolve;
			lock (bucket.Lock) {
				toResolve = [.. bucket.Pending];
				bucket.Pending.Clear();
			}
			foreach (var t in toResolve) t.TrySetException(ex);
		}
	}

	private sealed class WaiterBucket {
		public object Lock { get; } = new();
		public List<TaskCompletionSource> Pending { get; } = [];
		public bool Completed { get; set; }
	}
}
