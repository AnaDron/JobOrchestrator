using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Helper-методы публикации в <see cref="ChannelWriter{T}"/> с уважением к bounded-channel backpressure.
/// <para>
/// <b>Жёсткий контракт backpressure.</b> Fast path — <see cref="ChannelWriter{T}.TryWrite"/>; если
/// bounded-channel (capacity 10 000) заполнен, caller-thread <b>синхронно блокируется</b> через
/// <c>WriteAsync().GetAwaiter().GetResult()</c> до освобождения слота. Это намеренно: лучше
/// затормозить producer (runner, DueScanner, <see cref="IJobContextSink"/>), чем потерять событие
/// или раздуть очередь без границ. Вызывать только с потоков, где блокировка допустима
/// (ThreadPool runner, не UI/ASP.NET request thread).
/// </para>
/// <para>
/// <c>ChannelClosedException</c> поглощается после shutdown/fault — событие в закрытый channel
/// не ставится в очередь; completion-события при shutdown дочищаются в <see cref="EventLoop.DrainPendingRequests"/>.
/// </para>
/// </summary>
internal static class ChannelWriterExtensions {
	/// <summary>
	/// Синхронная публикация. Fast path — <see cref="ChannelWriter{T}.TryWrite"/>; при заполненном
	/// bounded-channel блокирует caller-thread через <c>WriteAsync().GetResult()</c> до освобождения места.
	/// <c>ChannelClosedException</c> поглощается (orchestrator stopped/faulted — события дальше не нужны).
	/// </summary>
	public static void Publish<T>(this ChannelWriter<T> writer, T item) {
		try {
			if (writer.TryWrite(item)) return;
			writer.WriteAsync(item).AsTask().GetAwaiter().GetResult();
		} catch (ChannelClosedException) {
			// Orchestrator shutdown/faulted — событие больше не нужно.
		}
	}
}
