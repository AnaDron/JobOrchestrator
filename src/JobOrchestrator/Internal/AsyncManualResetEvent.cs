namespace JobOrchestrator.Internal;

/// <summary>
/// Lock-free async-аналог <see cref="System.Threading.ManualResetEventSlim"/>: <see cref="Set"/>
/// резолвит ожидающий <see cref="Task"/>, <see cref="Reset"/> CAS-заменяет его на свежий pending.
/// <para>
/// Реализована для <see cref="DueScanner"/> в качестве wake-up-сигнала: producer-ов (event loop,
/// StageRunner finally, RegisterKey API) — много; consumer (scanner loop) — один. Альтернатива
/// «новый CTS + Cancel» дешевле по моральной нагрузке, но требует lock на смену ссылки и страдает
/// от <c>ObjectDisposedException</c> при race на Dispose. Этот паттерн обходит обе проблемы:
/// </para>
/// <list type="bullet">
/// <item>Замена слота — atomic <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/>.</item>
/// <item>TCS не «токсичен» после <c>TrySetResult</c> — двойной Set безопасно идемпотентен.</item>
/// <item>Нет Dispose — нет race-окна.</item>
/// </list>
/// </summary>
internal sealed class AsyncManualResetEvent {
	private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>
	/// Текущее ожидание. Завершается при ближайшем <see cref="Set"/>. Если caller-у нужны timeout
	/// и/или CancellationToken, навесить через <see cref="Task.WaitAsync(TimeSpan,TimeProvider,CancellationToken)"/>.
	/// </summary>
	public Task WaitAsync() => Volatile.Read(ref _tcs).Task;

	/// <summary>
	/// Сигнализирует ожидающих. Идемпотентно: повторный Set до Reset — no-op.
	/// </summary>
	public void Set() => Volatile.Read(ref _tcs).TrySetResult();

	/// <summary>
	/// Если текущий slot уже completed, CAS-заменяет его на свежий pending. Если slot ещё pending —
	/// no-op (зачем плодить waiters). Безопасно вызывать из любого потока.
	/// </summary>
	public void Reset() {
		var prev = Volatile.Read(ref _tcs);
		if (!prev.Task.IsCompleted) return;
		var fresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		// CAS-loser игнорируется: значит, другой Reset уже произвёл замену — слот свежий.
		Interlocked.CompareExchange(ref _tcs, fresh, prev);
	}
}
