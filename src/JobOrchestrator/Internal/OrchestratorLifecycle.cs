using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Singleton, координирующий fault-состояние оркестратора между
/// <see cref="JobOrchestratorHostedService"/> (выставляет флаг при крахе event loop)
/// и <see cref="JobOrchestratorRuntime"/> (читает флаг для fail-fast в TriggerAsync/RegisterKey/...).
/// </summary>
internal sealed class OrchestratorLifecycle(Channel<OrchestratorEvent> channel) {
	private volatile bool _faulted;

	public bool IsFaulted => _faulted;

	/// <summary>
	/// Выставляет fault-флаг и закрывает Channel — все ожидающие <c>WriteAsync</c> завалятся
	/// <see cref="ChannelClosedException"/>, последующие <c>TryWrite</c> вернут false. Это даёт fail-fast
	/// для внешних вызовов после краха или shutdown.
	/// </summary>
	public void MarkFaulted() {
		_faulted = true;
		channel.Writer.TryComplete();
	}

	/// <summary>Закрывает Channel без выставления Faulted (для нормального shutdown).</summary>
	public void CloseChannel() {
		channel.Writer.TryComplete();
	}
}
