using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Production-реализация <see cref="IJobContextSink"/>: публикует <see cref="KeyAddedEvent"/>/
/// <see cref="KeyRemovedEvent"/> в event-loop Channel с привязкой к инстансу-эмитеру
/// (<see cref="Instance"/>).
/// <para>
/// <b>Один sink на Instance lifetime</b> (pre-created в <see cref="InstanceCreator"/>):
/// Source и ChannelWriter постоянны → нет смысла создавать sink на каждую итерацию.
/// </para>
/// <para>
/// <b>Throws на closed-channel.</b> Если ChannelWriter закрыт (orchestrator после shutdown / faulted),
/// <see cref="AddKeyAsync"/>/<see cref="RemoveKeyAsync"/> бросают <see cref="InvalidOperationException"/> —
/// пользователю-сервису видна реальная причина «ключ не принят». Silently-drop здесь опасен:
/// сервис мог рассчитывать, что ключ propagate'нулся, и продолжит работать с фиктивно-зарегистрированным
/// state.
/// </para>
/// <para>
/// <b>Fast path / slow path.</b> 99.99% вызовов уходят через <see cref="ChannelWriter{T}.TryWrite"/>
/// и возвращают синхронно завершённый <see cref="ValueTask"/> без аллокации. При полном bounded-channel
/// caller получает honest async-ожидание через <see cref="ChannelWriter{T}.WriteAsync"/> — естественный
/// backpressure без блокировки runner-потока.
/// </para>
/// </summary>
internal sealed class ChannelJobContextSink(
	ChannelWriter<OrchestratorEvent> writer,
	Instance source
) : IJobContextSink {
	public ValueTask AddKeyAsync(string key, CancellationToken ct = default) =>
		PublishAsync(new KeyAddedEvent(source, key), "AddKey", key, ct);

	public ValueTask RemoveKeyAsync(string key, CancellationToken ct = default) =>
		PublishAsync(new KeyRemovedEvent(source, key), "RemoveKey", key, ct);

	private ValueTask PublishAsync(OrchestratorEvent evt, string operation, string key, CancellationToken ct) {
		try {
			if (writer.TryWrite(evt)) return ValueTask.CompletedTask;
		} catch (ChannelClosedException ex) {
			throw new InvalidOperationException(BuildClosedMessage(operation, key), ex);
		}
		return PublishSlowAsync(evt, operation, key, ct);
	}

	private async ValueTask PublishSlowAsync(OrchestratorEvent evt, string operation, string key, CancellationToken ct) {
		try {
			await writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException ex) {
			throw new InvalidOperationException(BuildClosedMessage(operation, key), ex);
		}
	}

	private string BuildClosedMessage(string operation, string key) =>
		$"Оркестратор остановлен; {operation}(\"{key}\") для инстанса {source.FullyQualifiedName} не может быть принят.";
}
