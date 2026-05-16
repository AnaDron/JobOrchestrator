using System.Collections.Concurrent;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр ожиданий <see cref="IInstanceHandle.WaitForSuccessAsync"/>: один
/// <see cref="TaskCompletionSource"/> на <see cref="InstanceIdentity"/>. Lock-free через
/// <see cref="ConcurrentDictionary{TKey,TValue}"/>.
/// <para>
/// <b>Алгоритм.</b> <see cref="Register"/> и <see cref="SignalSuccess"/> оба используют
/// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey, Func{TKey,TValue})"/> — порядок не важен:
/// </para>
/// <list type="bullet">
/// <item>Register пришёл первым → создал pending-TCS, дальнейший Signal сделает <c>TrySetResult</c>.</item>
/// <item>Signal пришёл первым → создал completed-TCS (<c>TrySetResult</c> ещё на свежесозданном TCS);
///       последующий Register увидит уже-завершённый Task через memoization slot'а.</item>
/// </list>
/// <para>
/// <b>Bucket-key — <see cref="InstanceIdentity"/></b>: Equals/GetHashCode определены через
/// <c>(Stage.Name, EncodedKey)</c>, поэтому lookup корректен независимо от того, какой именно
/// instance передан.
/// </para>
/// <para>
/// <b>Корректность под race:</b>
/// </para>
/// <list type="number">
/// <item><c>TrySetResult</c> идемпотентен: повторный Signal на ту же identity — no-op.</item>
/// <item>Cancel заявителя обслуживается через <see cref="Task.WaitAsync(CancellationToken)"/> —
///       только конкретный caller получает <c>OperationCanceledException</c>, slot и другие waiters не страдают.</item>
/// <item>Флаг <c>_stopped</c> закрывает регистрацию после <see cref="FailAll"/>; <b>re-check после
///       <c>GetOrAdd</c></b> гарантирует, что новый Register, проскочивший first-check, всё равно
///       получит exception (его TCS будет завершён через TrySetException и/или через snapshot в FailAll).</item>
/// </list>
/// </summary>
internal sealed class SuccessWaiters {
	private readonly ConcurrentDictionary<InstanceIdentity, TaskCompletionSource> _slots = new();
	private volatile bool _stopped;
	private Exception? _stopReason;

	public Task Register(InstanceIdentity identity, CancellationToken ct) {
		ct.ThrowIfCancellationRequested();
		if (_stopped) return Task.FromException(_stopReason!);

		var tcs = _slots.GetOrAdd(identity, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
		// Re-check после GetOrAdd: если FailAll выставил _stopped между first-check и GetOrAdd,
		// snapshot в FailAll мог не увидеть только что добавленный slot — TrySetException здесь
		// закрывает этот race.
		if (_stopped) tcs.TrySetException(_stopReason!);

		return tcs.Task.WaitAsync(ct);
	}

	/// <summary>Сигнализирует success — мемоизирует через TCS-slot, резолвит любых pending waiters.</summary>
	public void SignalSuccess(InstanceIdentity identity) {
		if (_stopped) return;
		var tcs = _slots.GetOrAdd(identity, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
		tcs.TrySetResult();
	}

	/// <summary>
	/// Удаляет slot и завершает любых pending waiters исключением. Вызывается, когда инстанс удалён
	/// каскадом — дальнейшее ожидание success бессмысленно. Будущие Register на ту же Identity
	/// (если её переиспользует новый инстанс) начнут с чистого состояния.
	/// </summary>
	public void SignalCancellation(InstanceIdentity identity, Exception ex) {
		if (_slots.TryRemove(identity, out var tcs)) tcs.TrySetException(ex);
	}

	/// <summary>
	/// Завершает все pending исключением и блокирует будущие Register-ы. Вызывается при shutdown/fault.
	/// </summary>
	public void FailAll(Exception ex) {
		_stopReason = ex;
		_stopped = true;
		// Snapshot+Clear: GetEnumerator() ConcurrentDictionary даёт moment-in-time snapshot. После Clear
		// возможно, что Register успел положить новый slot между snapshot и Clear — для него работает
		// re-check после GetOrAdd в Register.
		foreach (var kv in _slots) {
			kv.Value.TrySetException(ex);
		}
		_slots.Clear();
	}
}
