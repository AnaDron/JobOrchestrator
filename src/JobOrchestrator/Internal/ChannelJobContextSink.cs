using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Production-реализация <see cref="IJobContextSink"/>: публикует <see cref="KeyAddedEvent"/>/
/// <see cref="KeyRemovedEvent"/> в event-loop Channel с привязкой к инстансу-эмитеру
/// (<see cref="StageInstance"/>).
/// <para>
/// <b>Один sink на StageInstance lifetime</b> (pre-created в <see cref="InstanceCreator"/>):
/// Source и ChannelWriter постоянны → нет смысла создавать sink на каждую итерацию.
/// Один Sink-объект живёт столько же, сколько Instance.
/// </para>
/// <para>
/// <b>Throws на closed-channel.</b> Если ChannelWriter закрыт (orchestrator после shutdown / faulted),
/// <see cref="AddKey"/>/<see cref="RemoveKey"/> бросают <see cref="InvalidOperationException"/> —
/// пользователю-сервису видна реальная причина «ключ не принят». Silently-drop здесь опасен:
/// сервис мог рассчитывать, что ключ propagate'нулся, и продолжит работать с фиктивно-зарегистрированным
/// state.
/// </para>
/// </summary>
internal sealed class ChannelJobContextSink(
	ChannelWriter<OrchestratorEvent> writer,
	StageInstance source
) : IJobContextSink {
	public void AddKey(string key) => PublishOrThrow(new KeyAddedEvent(source, key), "AddKey");

	public void RemoveKey(string key) => PublishOrThrow(new KeyRemovedEvent(source, key), "RemoveKey");

	private void PublishOrThrow(OrchestratorEvent evt, string operation) {
		try {
			if (writer.TryWrite(evt)) return;
			writer.WriteAsync(evt).AsTask().GetAwaiter().GetResult();
		} catch (ChannelClosedException ex) {
			throw new InvalidOperationException(
				$"Оркестратор остановлен; {operation}(\"{(evt is KeyAddedEvent ka ? ka.Key : ((KeyRemovedEvent)evt).Key)}\") для инстанса {source.FullyQualifiedName} не может быть принят.",
				ex);
		}
	}
}
