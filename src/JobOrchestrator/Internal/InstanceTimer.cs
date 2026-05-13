using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>Per-инстансовый таймер: один callback в <see cref="ScheduleAt"/>, публикует <see cref="TimerTickedEvent"/>.</summary>
internal sealed class InstanceTimer : IDisposable {
	private readonly StageInstance _instance;
	private readonly ChannelWriter<OrchestratorEvent> _events;
	private readonly Timer _timer;
	private bool _disposed;

	public InstanceTimer(StageInstance instance, ChannelWriter<OrchestratorEvent> events) {
		_instance = instance;
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
		// Race: между fire timer-а и Dispose может проскочить один последний callback —
		// проверяем флаг, чтобы не отправлять «прощальный» tick.
		if (_disposed) return;
		_events.Publish(new TimerTickedEvent(_instance));
	}
}
