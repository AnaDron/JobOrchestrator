using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>Per-инстансовый таймер: один callback в <see cref="ScheduleAt"/>, публикует <see cref="TimerTickedEvent"/>.</summary>
internal sealed class InstanceTimer(StageInstance instance, ChannelWriter<OrchestratorEvent> events) : IDisposable {
	private readonly Timer _timer = new(
		_ => events.TryWrite(new TimerTickedEvent(instance)),
		null, Timeout.Infinite, Timeout.Infinite);
	private bool _disposed;

	/// <summary>Запланировать тик в указанное время по <c>Environment.TickCount64</c>.</summary>
	public void ScheduleAt(long ticksMs) {
		if (_disposed) return;
		long delay = Math.Max(0, ticksMs - Environment.TickCount64);
		_timer.Change(delay, Timeout.Infinite);
	}

	public void Dispose() {
		if (_disposed) return;
		_disposed = true;
		_timer.Dispose();
	}
}
