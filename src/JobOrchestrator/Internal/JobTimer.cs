using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>Per-инстансовый таймер: один callback в <see cref="ScheduleAt"/>, публикует <see cref="TimerTickedEvent"/>.</summary>
internal sealed class JobTimer : IDisposable {
	private readonly Job _job;
	private readonly ChannelWriter<OrchestratorEvent> _events;
	private readonly Timer _timer;
	private bool _disposed;

	public JobTimer(Job job, ChannelWriter<OrchestratorEvent> events) {
		_job = job;
		_events = events;
		_timer = new Timer(OnTick, null, Timeout.Infinite, Timeout.Infinite);
	}

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

	private void OnTick(object? _) {
		if (_disposed) return;
		_events.TryWrite(new TimerTickedEvent(_job));
	}
}
