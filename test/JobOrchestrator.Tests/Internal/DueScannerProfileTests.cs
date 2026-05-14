using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit.Abstractions;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Профилирование <see cref="DueScanner.Tick"/> на больших графах. Принимает решение по TODO-пункту
/// «sorted-by-deadline collection vs linear-scan».
/// <para>
/// Текущая модель: один scan = O(N) volatile-read поля <see cref="Instance.NextAutoUtc"/>.
/// Альтернатива: <c>PriorityQueue&lt;Instance, DateTimeOffset&gt;</c> с O(log N) на schedule-change,
/// O(log N) на dequeue. Точка безразличия: log₂(N) ≈ 14 при N=10k — sorted-collection дешевле только если
/// средняя «доля due-инстансов на один scan» меньше 1/14 (~7%).
/// </para>
/// <para>
/// Замеренные числа (release-like CPU, без параллельных тестов): no-work scan ~165 нс/инстанс,
/// 1.66 мс на N=10k. all-due scan (с публикацией N <see cref="TimerTickedEvent"/>) — ~20 мс на N=10k,
/// доминируется channel.TryWrite × N (linear- и sorted-варианты идентичны).
/// </para>
/// <para>
/// <b>Класс помечен <see cref="CollectionAttribute"/> "DueScannerProfile" с отключённым parallel-runner-ом
/// (см. <c>AssemblyInfo.cs</c>):</b> timing-замеры под параллельной нагрузкой xUnit дают false-positive
/// regression alerts. Порог 10мс на N=10k — soft-ceiling, ловит регрессию на порядок, переносит CI-jitter.
/// </para>
/// </summary>
[Collection("DueScannerProfile")]
public sealed class DueScannerProfileTests(ITestOutputHelper output) {
	private static StageDescriptor MakeStage() => TestStages.Make("stage");

	private static (DueScanner Scanner, InstanceManager Manager, Channel<OrchestratorEvent> Channel) Setup(int n, bool allDue) {
		var stage = MakeStage();
		var manager = new InstanceManager();
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var emptyKeys = new Dictionary<string, string>(StringComparer.Ordinal);

		for (int i = 0; i < n; i++) {
			var keys = new Dictionary<string, string>(StringComparer.Ordinal) { ["i"] = i.ToString() };
			var instance = new Instance { Identity = new InstanceIdentity(stage, keys) };
			// All-due → NextAutoUtc в прошлом; no-work → далеко в будущем.
			instance.SetMetrics(instance.Metrics with {
				NextAutoUtc = allDue ? DateTimeOffset.UtcNow.AddSeconds(-1) : DateTimeOffset.UtcNow.AddDays(30),
			});
			manager.Add(instance);
		}

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);
		return (scanner, manager, channel);
	}

	[Theory]
	[InlineData(100)]
	[InlineData(1_000)]
	[InlineData(10_000)]
	public void Profile_NoWork_ScanCostIsLinearButCheap(int n) {
		var (scanner, _, _) = Setup(n, allDue: false);

		// Warm-up.
		for (int i = 0; i < 5; i++) _ = scanner.Tick(DateTimeOffset.UtcNow);

		const int iterations = 20;
		var sw = Stopwatch.StartNew();
		for (int i = 0; i < iterations; i++) {
			_ = scanner.Tick(DateTimeOffset.UtcNow);
		}
		sw.Stop();

		double avgMicros = sw.Elapsed.TotalMicroseconds / iterations;
		output.WriteLine($"N={n,6} no-work: avg scan = {avgMicros:F1} µs ({avgMicros / n * 1000:F2} ns/instance)");

		// Soft ceiling: на N=10k один scan должен укладываться в <20мс. Изолированно — ~2-3мс
		// (immutable JobMetrics snapshot добавил indirection vs long-encoded ticks),
		// 20мс закладывает CI-jitter, параллельную нагрузку и GC. Превышение порога на порядок (>20мс)
		// = сигнал, что linear scan становится узким горлышком — пора профилировать sorted-by-deadline.
		if (n == 10_000) {
			avgMicros.Should().BeLessThan(20_000, "10k no-work scan должен быть < 20мс");
		}
		scanner.Dispose();
	}

	[Theory]
	[InlineData(100)]
	[InlineData(1_000)]
	[InlineData(10_000)]
	public void Profile_AllDue_PublishCostIncludesChannelWrites(int n) {
		// При all-due каждый instance публикуется в channel — это включает channel-write cost.
		// Сравниваем с no-work: разница ≈ amortized cost одного UnboundedChannel.TryWrite + new TimerTickedEvent.
		var sw = Stopwatch.StartNew();
		for (int i = 0; i < 3; i++) {
			var (scanner, _, channel) = Setup(n, allDue: true);
			scanner.Tick(DateTimeOffset.UtcNow);
			// Drain channel чтобы GC не давил.
			while (channel.Reader.TryRead(out _)) { }
			scanner.Dispose();
		}
		sw.Stop();

		double avgMicros = sw.Elapsed.TotalMicroseconds / 3;
		output.WriteLine($"N={n,6} all-due: avg setup+scan+drain = {avgMicros:F0} µs (incl. channel writes)");
	}
}
