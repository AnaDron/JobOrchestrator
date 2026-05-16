namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForSuccessAsync"/>: одна группа waiter-ов на
/// <see cref="InstanceIdentity"/>. Сигнал об успехе резолвит все накопленные TCS этой группы и
/// мемоизирует «уже отрабатывал» в отдельном <c>_completedOnce</c> — late-register, прибежавший ПОСЛЕ
/// сигнала, получит уже-завершённый Task без bucket-аллокации.
/// <para>
/// Bucket-key — <see cref="InstanceIdentity"/>: Equals/GetHashCode уже определены через
/// <c>(Stage.Name, EncodedKey)</c>, поэтому Dictionary-lookup корректен независимо от того,
/// какой именно instance передан (любые две Identity с одинаковыми Stage+EncodedKey считаются
/// равными).
/// </para>
/// <para>
/// <b>Bucket lifecycle.</b> <see cref="WaiterBucket"/> существует ТОЛЬКО пока есть pending TCS.
/// <see cref="SignalSuccess"/> для identity, на которую никто не подписан, не аллоцирует bucket —
/// просто добавляет identity в <c>_completedOnce</c>. Это удерживает <c>_buckets</c> компактным
/// для long-lived keyless-инстансов, которых никто не ждёт.
/// </para>
/// <para>
/// Корректность под race:
/// </para>
/// <list type="number">
/// <item><see cref="SignalSuccess"/> сначала отмечает <c>_completedOnce</c>, потом резолвит bucket →
///       <see cref="Register"/>, прилетевший ПОСЛЕ сигнала, увидит флаг и резолвится сразу.</item>
/// <item>Флаг <c>_stopped</c> закрывает регистрацию после <see cref="FailAll"/>: новые caller-ы
///       немедленно получают исключение.</item>
/// <item>CancellationToken: register-cleanup снимает TCS из bucket'а при отмене — не накапливаем
///       отменённые ссылки.</item>
/// </list>
/// </summary>
internal sealed class SuccessWaiters {
	private readonly object _gate = new();
	private readonly Dictionary<InstanceIdentity, WaiterBucket> _buckets = new();
	private readonly HashSet<InstanceIdentity> _completedOnce = [];
	private bool _stopped;
	private Exception? _stopReason;

	public Task Register(InstanceIdentity identity, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		WaiterBucket bucket;
		lock (_gate) {
			if (_stopped) {
				tcs.TrySetException(_stopReason!);
				return tcs.Task;
			}
			if (_completedOnce.Contains(identity)) {
				tcs.TrySetResult();
				return tcs.Task;
			}
			if (!_buckets.TryGetValue(identity, out var existing)) {
				existing = new WaiterBucket();
				_buckets[identity] = existing;
			}
			bucket = existing;
			bucket.Pending.Add(tcs);
		}

		if (ct.CanBeCanceled) {
			var reg = ct.Register(state => {
				var t = (TaskCompletionSource)state!;
				if (t.TrySetCanceled()) {
					lock (_gate) bucket.Pending.Remove(t);
				}
			}, tcs);
			tcs.Task.ContinueWith(static (_, r) => ((CancellationTokenRegistration)r!).Dispose(),
				reg, TaskScheduler.Default);
		}

		return tcs.Task;
	}

	/// <summary>Сигнализирует success — резолвит всех pending и мемоизирует identity для late-register'ов.</summary>
	public void SignalSuccess(InstanceIdentity identity) {
		TaskCompletionSource[] toResolve;
		lock (_gate) {
			_completedOnce.Add(identity);
			if (!_buckets.Remove(identity, out var bucket)) return;
			toResolve = [.. bucket.Pending];
			bucket.Pending.Clear();
		}
		foreach (var t in toResolve) t.TrySetResult();
	}

	/// <summary>
	/// Удаляет bucket и memoized-флаг, завершает все pending исключением. Вызывается, когда инстанс
	/// удалён каскадом — дальнейшее ожидание success бессмысленно. Будущие Register-ы на ту же Identity
	/// (если её переиспользует новый инстанс) начнут с чистого состояния.
	/// </summary>
	public void SignalCancellation(InstanceIdentity identity, Exception ex) {
		TaskCompletionSource[] toResolve;
		lock (_gate) {
			_completedOnce.Remove(identity);
			if (!_buckets.Remove(identity, out var bucket)) return;
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
			_completedOnce.Clear();
		}
		foreach (var bucket in all) {
			TaskCompletionSource[] toResolve;
			lock (_gate) {
				toResolve = [.. bucket.Pending];
				bucket.Pending.Clear();
			}
			foreach (var t in toResolve) t.TrySetException(ex);
		}
	}

	private sealed class WaiterBucket {
		public List<TaskCompletionSource> Pending { get; } = [];
	}
}
