using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Production-реализация <see cref="IJobContextSink"/>: публикует <see cref="KeyAddedEvent"/>/
/// <see cref="KeyRemovedEvent"/> в event-loop Channel с привязкой к инстансу-эмитеру
/// (<see cref="StageInstance"/>). Каждый sink-экземпляр обслуживает ровно одну итерацию
/// одного инстанса — он одноразовый, создаётся в <see cref="StageRunner"/> на каждый <c>ExecuteAsync</c>.
/// </summary>
internal sealed class ChannelJobContextSink(
	ChannelWriter<OrchestratorEvent> writer,
	StageInstance source
) : IJobContextSink {
	public void AddKey(string key) {
		writer.Publish(new KeyAddedEvent(source.Stage.Name, key, source));
	}

	public void RemoveKey(string key) {
		writer.Publish(new KeyRemovedEvent(source.Stage.Name, key, source));
	}
}
