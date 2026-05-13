using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Helper-методы публикации в <see cref="ChannelWriter{T}"/> с уважением к bounded-channel backpressure
/// и без silent loss событий. В отличие от прямого <see cref="ChannelWriter{T}.TryWrite"/>,
/// который при <c>FullMode = Wait</c> bounded-channel возвращает <c>false</c> молча, эти методы
/// либо ждут места (синхронно блокируя поток), либо корректно поглощают <c>ChannelClosedException</c>
/// после <see cref="OrchestratorLifecycle.MarkFaulted"/>/<see cref="OrchestratorLifecycle.CloseChannel"/>.
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
