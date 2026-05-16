namespace JobOrchestrator.Abstractions;

/// <summary>
/// Канал распространения keyspace-операций <c>AddKey</c>/<c>RemoveKey</c> из <see cref="IJobService.ExecuteAsync"/>
/// наружу — в event loop оркестратора. Отделён от <see cref="JobContext"/> для тестируемости:
/// в unit-тестах сервиса можно подменить sink на mock и проверить, какие операции вызывались.
/// <para>
/// В production реализуется внутренним <c>ChannelSink</c>, который публикует <c>KeyAddedEvent</c>/
/// <c>KeyRemovedEvent</c> в <c>Channel&lt;OrchestratorEvent&gt;</c> с привязкой к инстансу-эмитеру.
/// </para>
/// <para>
/// <b>Async-контракт.</b> Операции возвращают <see cref="ValueTask"/> — fast path (свободный слот
/// в bounded channel) синхронно резолвится в <see cref="ValueTask.CompletedTask"/> без аллокации,
/// а при заполненной очереди caller естественно <c>await</c>-ит освободившийся слот вместо
/// sync-over-async блокировки runner-потока.
/// </para>
/// </summary>
public interface IJobContextSink {
	/// <summary>
	/// Регистрирует ключ в keyspace текущей стадии. Идемпотентно.
	/// <para>
	/// Обычно мгновенно (<see cref="System.Threading.Channels.ChannelWriter{T}.TryWrite"/>); при
	/// переполнении очереди event loop (10 000 событий) <c>await</c> ждёт backpressure-слота —
	/// runner-поток не блокируется sync-over-async.
	/// </para>
	/// </summary>
	ValueTask AddKeyAsync(string key, CancellationToken ct = default);

	/// <summary>
	/// Удаляет ключ из keyspace текущей стадии. Каскадно отменяет зависимых. Идемпотентно.
	/// Семантика backpressure — как у <see cref="AddKeyAsync"/>.
	/// </summary>
	ValueTask RemoveKeyAsync(string key, CancellationToken ct = default);
}
