using System.Collections.Concurrent;
using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Per-subscriber обёртка над bounded <see cref="Channel{T}"/> для broadcaster-сценариев:
/// <see cref="IStageHandle.Changes"/> (T = <c>(StageChange, long)</c>) и <see cref="IInstanceHandle"/>-as-AsyncEnumerable
/// (T = <see cref="IIterationHandle"/>).
/// <para>
/// <b>Lifecycle:</b> <see cref="IAsyncDisposable"/> — caller использует <c>await using</c>, RAII удаляет
/// subscriber из owner-коллекции и завершает канал. <see cref="Complete"/> — owner-side bulk-finish
/// (cascade-removal / shutdown), асимметрично инициируется owner'ом.
/// </para>
/// <para>
/// <b>Backpressure:</b> <see cref="BoundedChannelFullMode.DropOldest"/> — медленный consumer теряет старые
/// сообщения. <see cref="DropCount"/> учитывает реальные дропы через <c>itemDropped</c>-callback
/// (TryWrite при DropOldest сам по себе не сигнализирует о дропе — он всегда возвращает <c>true</c>, пока
/// канал жив; <c>false</c> бывает только после <see cref="Complete"/>).
/// </para>
/// </summary>
internal sealed class BoundedSubscriber<T> : IAsyncDisposable {
	private readonly ConcurrentDictionary<BoundedSubscriber<T>, byte> _ownerSet;
	private readonly Channel<T> _channel;
	private long _dropCount;

	public BoundedSubscriber(int bufferCapacity, ConcurrentDictionary<BoundedSubscriber<T>, byte> ownerSet) {
		_ownerSet = ownerSet;
		_channel = Channel.CreateBounded<T>(
			new BoundedChannelOptions(bufferCapacity) {
				SingleReader = true,
				SingleWriter = false,
				FullMode = BoundedChannelFullMode.DropOldest,
			},
			itemDropped: _ => Interlocked.Increment(ref _dropCount));
	}

	/// <summary>Число пропущенных сообщений из-за переполнения буфера.</summary>
	public long DropCount => Interlocked.Read(ref _dropCount);

	/// <summary>Reader-side для consumer-loop.</summary>
	public ChannelReader<T> Reader => _channel.Reader;

	/// <summary>
	/// Publish. При переполнении старейший элемент дропается внутри writer'а (DropOldest); счётчик
	/// инкрементится через itemDropped-callback. TryWrite=false возможен только после <see cref="Complete"/>
	/// — событие тихо отбрасывается (consumer уже ушёл).
	/// </summary>
	public void Publish(T item) => _channel.Writer.TryWrite(item);

	/// <summary>
	/// Owner-side bulk-finish: вызывается owner'ом при cascade-removal / shutdown — единовременно завершает
	/// канал, consumer выйдет из <c>ReadAllAsync</c> естественно. Subscriber остаётся зарегистрированным
	/// в owner-set (cleanup произойдёт через <see cref="DisposeAsync"/> со стороны consumer'а).
	/// </summary>
	public void Complete() => _channel.Writer.TryComplete();

	/// <summary>
	/// Subscriber-side cleanup: удаляет себя из owner-set и завершает канал. Synchronously по факту
	/// (TryRemove + TryComplete оба sync), <see cref="IAsyncDisposable"/> выбран для естественного
	/// <c>await using</c> в async-iterator контекстах.
	/// </summary>
	public ValueTask DisposeAsync() {
		_ownerSet.TryRemove(this, out _);
		_channel.Writer.TryComplete();
		return ValueTask.CompletedTask;
	}
}
