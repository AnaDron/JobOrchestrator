using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using JobOrchestrator.Configuration;

namespace JobOrchestrator.Internal;

/// <summary>
/// Singleton, координирующий fault-состояние оркестратора между
/// <see cref="JobOrchestratorHostedService"/> (выставляет флаг при крахе event loop)
/// и <see cref="JobOrchestratorRuntime"/> (читает флаг для fail-fast в RunAsync/RegisterKey/...).
/// <para>
/// Также владеет <see cref="WorkersCancellationToken"/> — токеном, который cancel-ится в момент
/// <see cref="MarkFaulted"/>. Используется <see cref="StageRunner"/> при создании linked CTS,
/// чтобы running-итерации получили cancel при крахе event loop (host shutdown cancel-ится отдельно
/// через BackgroundService.stoppingToken).
/// </para>
/// </summary>
[SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize",
	Justification = "Sealed class без финализатора — GC.SuppressFinalize был бы no-op и misleading.")]
internal sealed class OrchestratorLifecycle(Channel<OrchestratorEvent> channel) : IDisposable {
	private readonly CancellationTokenSource _workersCts = new();
	private volatile bool _faulted;
	private bool _disposed;

	public bool IsFaulted => _faulted;

	/// <summary>Cancel-ится при <see cref="MarkFaulted"/>. Не cancel-ится при graceful <see cref="CloseChannel"/>.</summary>
	public CancellationToken WorkersCancellationToken => _workersCts.Token;

	/// <summary>
	/// Выставляет fault-флаг, отменяет running workers и закрывает Channel — внешние вызовы получают fail-fast.
	/// </summary>
	public void MarkFaulted() {
		_faulted = true;
		try { _workersCts.Cancel(); } catch (ObjectDisposedException) { }
		channel.Writer.TryComplete();
	}

	/// <summary>Закрывает Channel без выставления Faulted и без cancel running workers (graceful shutdown).</summary>
	public void CloseChannel() {
		channel.Writer.TryComplete();
	}

	/// <summary>
	/// Отменяет running-итерации без fault-флага (graceful shutdown после
	/// <see cref="JobOrchestratorHostOptions.ShutdownIterationTimeout"/>).
	/// </summary>
	public void CancelRunningWorkers() {
		try { _workersCts.Cancel(); } catch (ObjectDisposedException) { }
	}

	public void Dispose() {
		if (_disposed) return;
		_disposed = true;
		_workersCts.Dispose();
	}
}
