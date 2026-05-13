using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Pull-based планировщик: один background-loop вместо per-инстансового <see cref="Timer"/>.
/// <para>
/// Алгоритм:
/// </para>
/// <list type="number">
/// <item>Снимок всех инстансов через <see cref="InstanceManager.All"/> (lock-free, <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}.Values"/>).</item>
/// <item>Для каждого инстанса в <see cref="InstanceLifecycleState.Idle"/> с <c>NextAutoUtc &lt;= now</c> — публикуем <see cref="TimerTickedEvent"/>.</item>
/// <item>Находим минимум <c>NextAutoUtc</c> среди оставшихся (не-Running, не-due). Засыпаем до этого момента.</item>
/// <item>Сон прерывается через wake-up CTS — <see cref="Wake"/> вызывается event loop'ом, когда расписание укорочено
///       (новый инстанс, retry-shorter-than-interval, и т. п.).</item>
/// </list>
/// <para>
/// Trade-off versus per-instance <see cref="Timer"/>: один аллокированный таймер вместо N, одна
/// синхронизация per-tick (cheap volatile-read) вместо N callbacks через ThreadPool.
/// </para>
/// <para>
/// <b>Profile-decision (см. <c>DueScannerProfileTests</c>):</b> linear scan на N=10k = ~1.66 мс
/// (~165 нс/инстанс, кэш-friendly последовательный volatile-read). Sorted-by-deadline
/// (<c>PriorityQueue&lt;TInst, DateTimeOffset&gt;</c>) даёт O(log N) на find-min, но ухудшает hot-path
/// перепланирования (StageCompleted/Failed → O(log N) re-insert вместо одного atomic-write
/// в <c>NextAutoUtc</c>) и не уменьшает доминирующую стоимость publish-pass (все due-инстансы
/// в любом случае попадают в channel). Решение: linear scan; пересмотреть при N &gt; 50k или
/// при появлении сценариев &gt; 100 due-events/сек.
/// </para>
/// </summary>
internal sealed class DueScanner(
	InstanceManager instances,
	Channel<OrchestratorEvent> channel,
	TimeProvider time,
	ILogger<DueScanner> logger
) : IDisposable {
	// Минимально допустимый интервал сна — защита от busy-loop при NextAutoUtc=now на куче инстансов.
	private static readonly TimeSpan MinSleep = TimeSpan.FromMilliseconds(1);
	// Максимальный интервал сна, если нет ни одного инстанса с NextAutoUtc — просыпаемся периодически
	// проверить, не создались ли новые (на случай, если Wake() пропустили race-condition).
	private static readonly TimeSpan MaxSleep = TimeSpan.FromSeconds(30);

	private CancellationTokenSource _wakeCts = new();
	private readonly object _wakeLock = new();
	private bool _disposed;

	/// <summary>Будит scanner: следующая итерация loop'а посмотрит на актуальный <c>NextAutoUtc</c>.</summary>
	public void Wake() {
		if (_disposed) return;
		CancellationTokenSource? toCancel;
		lock (_wakeLock) {
			toCancel = _wakeCts;
		}
		try { toCancel?.Cancel(); } catch (ObjectDisposedException) { /* race на Dispose */ }
	}

	public async Task RunAsync(CancellationToken stoppingToken) {
		logger.LogDebug("DueScanner started.");
		while (!stoppingToken.IsCancellationRequested) {
			var now = time.GetUtcNow();
			var nextDue = ScanAndPublishDue(now);

			TimeSpan sleep;
			if (nextDue is null) {
				sleep = MaxSleep;
			} else {
				var delta = nextDue.Value - now;
				sleep = delta < MinSleep ? MinSleep : delta > MaxSleep ? MaxSleep : delta;
			}

			CancellationTokenSource freshCts;
			lock (_wakeLock) {
				_wakeCts.Dispose();
				_wakeCts = freshCts = new CancellationTokenSource();
			}

			using var linked = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, freshCts.Token);
			try {
				await Task.Delay(sleep, time, linked.Token).ConfigureAwait(false);
			} catch (OperationCanceledException) {
				// Либо shutdown, либо Wake() — в обоих случаях просто продолжаем loop.
			}
		}
		logger.LogDebug("DueScanner stopped.");
	}

	/// <summary>
	/// Test/profile hook: однократный синхронный scan + publish, без внешнего loop'а. Открыт как
	/// <c>internal</c> для микробенчмарков и unit-тестов. Реальный path в production — через
	/// <see cref="RunAsync"/> с динамической задержкой и wake-up CTS.
	/// </summary>
	internal DateTimeOffset? Tick(DateTimeOffset now) => ScanAndPublishDue(now);

	/// <summary>
	/// Сканирует все инстансы, публикует <see cref="TimerTickedEvent"/> для due-инстансов,
	/// возвращает ближайший <c>NextAutoUtc</c> в будущем (или <c>null</c>, если расписаний нет).
	/// </summary>
	private DateTimeOffset? ScanAndPublishDue(DateTimeOffset now) {
		DateTimeOffset? nextDue = null;
		foreach (var instance in instances.All) {
			// Running-инстансы не тикают: их перепланирование произойдёт в StageCompleted/Failed handler-е.
			if (instance.State != InstanceLifecycleState.Idle) continue;
			var next = instance.NextAutoUtc;
			if (next is null) continue;
			if (next.Value <= now) {
				channel.Writer.Publish(new TimerTickedEvent(instance));
				continue;
			}
			if (nextDue is null || next.Value < nextDue.Value) nextDue = next;
		}
		return nextDue;
	}

	public void Dispose() {
		if (_disposed) return;
		_disposed = true;
		lock (_wakeLock) {
			_wakeCts.Dispose();
		}
	}
}
