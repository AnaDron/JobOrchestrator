using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Sync-обёртка над <see cref="ChannelWriter{T}.WriteAsync"/> для call-site'ов без <c>async</c>
/// (<see cref="DueScanner"/>, completion в <see cref="StageRunner"/>, внешний RegisterKey/UnregisterKey).
/// <para>
/// BCL <see cref="ChannelWriter{T}.WriteAsync"/> уже делает <see cref="ChannelWriter{T}.TryWrite"/>-fast-path
/// внутри (zero-alloc на success), поэтому отдельная async-обёртка не нужна — async-caller'ы зовут
/// <c>writer.WriteAsync(...)</c> напрямую. <see cref="Publish{T}"/> добавляет ровно две вещи поверх
/// BCL-семантики: (1) sync-block при переполнении вместо <c>await</c>; (2) <see cref="ChannelClosedException"/>
/// → <c>false</c> (вместо throw), чтобы call-site'ы при shutdown молча пропускали публикацию.
/// </para>
/// </summary>
internal static class ChannelWriterExtensions {
	public static bool Publish<T>(this ChannelWriter<T> writer, T item) {
		try {
			writer.WriteAsync(item).AsTask().GetAwaiter().GetResult();
			return true;
		} catch (ChannelClosedException) {
			return false;
		}
	}
}
