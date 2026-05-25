using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Pull-based scheduler-loop: один background-loop вместо per-инстансового <see cref="Timer"/>.
/// Запускается параллельно с event-consumer-loop'ом в <see cref="RunAsync"/> через <c>Task.Run</c>.
/// <para>
/// Алгоритм:
/// </para>
/// <list type="number">
/// <item>Снимок всех инстансов через <see cref="InstanceManager.All"/> (lock-free <see cref="System.Collections.Concurrent.ConcurrentDictionary{TKey,TValue}.Values"/>).</item>
/// <item>Для каждого инстанса в <see cref="InstanceLifecycleState.Idle"/> с <c>NextAutoUtc &lt;= now</c> — публикуем <see cref="TimerTickedEvent"/>.</item>
/// <item>Находим минимум <c>NextAutoUtc</c> среди оставшихся. Засыпаем до этого момента (либо <see cref="DueScanMaxSleep"/>).</item>
/// <item>Сон прерывается через <see cref="WakeDueScanner"/> — её зовёт event-loop, когда расписание укорочено.</item>
/// </list>
/// <para>
/// Trade-off versus per-instance <see cref="Timer"/>: один аллокированный таймер вместо N, одна
/// синхронизация per-tick (cheap volatile-read) вместо N callbacks через ThreadPool.
/// </para>
/// <para>
/// <b>Profile-decision (см. DueScannerProfileTests до merge):</b> linear scan на N=10k = ~1.66 мс
/// (~165 нс/инстанс, кэш-friendly последовательный volatile-read). Sorted-by-deadline даёт
/// O(log N) на find-min, но ухудшает hot-path перепланирования (StageCompleted/Failed → O(log N) re-insert
/// вместо одного atomic-write в <c>NextAutoUtc</c>) и не уменьшает доминирующую стоимость publish-pass
/// (все due-инстансы в любом случае попадают в channel). Решение: linear scan; пересмотреть при N &gt; 50k
/// или при появлении сценариев &gt; 100 due-events/сек.
/// </para>
/// </summary>
internal sealed partial class EventLoop {
	/// <summary>
	/// Будит due-scan loop: следующая итерация loop'а посмотрит на актуальный <c>NextAutoUtc</c>.
	/// Безопасно вызывать из любого потока (event-loop-consumer или ThreadPool runner-finally).
	/// </summary>
	private void WakeDueScanner() => _dueScanWake.Set();

	private async Task RunDueScannerAsync(CancellationToken stoppingToken) {
		Log.DueScannerStarted(logger, null);
		while (!stoppingToken.IsCancellationRequested) {
			// Reset ДО scan: если WakeDueScanner придёт во время ScanAndPublishDue, флаг взведётся
			// и следующий WaitAsync завершится мгновенно — wake-up не теряется.
			_dueScanWake.Reset();

			var now = time.GetUtcNow();
			WarnOnChannelBacklog(now);
			var nextDue = ScanAndPublishDue(now);

			TimeSpan sleep;
			if (nextDue is null) {
				sleep = DueScanMaxSleep;
			} else {
				var delta = nextDue.Value - now;
				sleep = delta < DueScanMinSleep ? DueScanMinSleep : delta > DueScanMaxSleep ? DueScanMaxSleep : delta;
			}

			try {
				await _dueScanWake.WaitAsync().WaitAsync(sleep, time, stoppingToken).ConfigureAwait(false);
			} catch (TimeoutException) {
				// Sleep истёк — нормальное продолжение loop'а.
			} catch (OperationCanceledException) {
				// Shutdown — внешний while проверит stoppingToken и завершится.
			}
		}
		Log.DueScannerStopped(logger, null);
	}

	/// <summary>
	/// Operational visibility: если очередь оркестратор-событий превысила <see cref="ChannelBacklogThreshold"/>,
	/// логирует warning с anti-spam suppression (<see cref="BacklogWarningInterval"/>).
	/// </summary>
	private void WarnOnChannelBacklog(DateTimeOffset now) {
		var reader = channel.Reader;
		if (!reader.CanCount) return;
		int count = reader.Count;
		if (count < ChannelBacklogThreshold) return;
		if (now - _lastBacklogWarning < BacklogWarningInterval) return;
		_lastBacklogWarning = now;
		Log.ChannelBacklog(logger, count, null);
	}

	/// <summary>
	/// Сканирует все инстансы, публикует <see cref="TimerTickedEvent"/> для due-инстансов,
	/// возвращает ближайший <c>NextAutoUtc</c> в будущем (или <c>null</c>, если расписаний нет).
	/// Идемпотентность через <see cref="Instance.TryAcquirePendingTick"/>: если флаг уже взведён
	/// (предыдущий tick ещё в Channel или обрабатывается consumer-ом), scan пропускает инстанс.
	/// </summary>
	private DateTimeOffset? ScanAndPublishDue(DateTimeOffset now) {
		DateTimeOffset? nextDue = null;
		foreach (var instance in instances.All) {
			if (instance.State != InstanceLifecycleState.Idle) continue;
			var next = instance.Metrics.Schedule.NextAutoUtc;
			if (next is null) continue;
			if (next.Value <= now) {
				if (!instance.TryAcquirePendingTick()) continue;
				try {
					channel.Writer.Publish(new TimerTickedEvent(instance));
				} catch {
					instance.ReleasePendingTick();
					throw;
				}
				continue;
			}
			if (nextDue is null || next.Value < nextDue.Value) nextDue = next;
		}
		return nextDue;
	}
}
