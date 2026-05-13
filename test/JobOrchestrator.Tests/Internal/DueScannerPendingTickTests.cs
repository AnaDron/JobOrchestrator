using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Идемпотентность <see cref="DueScanner.Tick"/>: повторный scan не должен публиковать дубль
/// <see cref="TimerTickedEvent"/>, пока предыдущий ещё не обработан consumer-ом.
/// <para>
/// Сценарий race в production: между snapshot и BeginIteration (где State=Running и NextAutoUtc=null
/// взводятся в consumer-thread) проходит несколько мс. Если за это время DueScanner успеет сделать
/// ещё один scan, он опять увидит <c>State == Idle &amp;&amp; NextAutoUtc &lt;= now</c> и опубликует
/// второй TimerTicked. EventLoop тогда обработает первый → BeginIteration → State=Running, и второй
/// уйдёт в TryAccept → <see cref="TriggerResult.AlreadyRunning"/> — не катастрофа, но шум в логе и
/// потраченная обработка channel-события.
/// </para>
/// </summary>
public sealed class DueScannerPendingTickTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage() => new() {
		Name = "x",
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = [],
	};

	[Fact]
	public void Tick_TwicedWithSameDueInstance_PublishesOnlyOneTimerTicked() {
		var manager = new InstanceManager();
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var stage = MakeStage();
		var empty = new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new StageInstance { Identity = new InstanceIdentity(stage, empty) };
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1) });   // due
		manager.Add(inst);

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);

		// Два последовательных Tick'а на одно и то же due-окно. State пока Idle, consumer (имитируемый)
		// не успел поднять State=Running и сбросить NextAutoUtc. Второй scan не должен публиковать
		// повторный TimerTicked для того же инстанса.
		scanner.Tick(DateTimeOffset.UtcNow);
		scanner.Tick(DateTimeOffset.UtcNow);

		var published = new List<TimerTickedEvent>();
		while (channel.Reader.TryRead(out var e)) {
			if (e is TimerTickedEvent tt) published.Add(tt);
		}
		published.Should().ContainSingle("pendingTick-CAS должен отсечь второй tick до обработки первого");
		scanner.Dispose();
	}

	[Fact]
	public void Tick_AfterIterationEnds_RepublishesOnNextDueWindow() {
		// После того как consumer обработал первый tick (имитируем через State=Idle + сброс NextAutoUtc),
		// затем перевзвёл NextAutoUtc на новое due-окно → scanner должен опять опубликовать tick.
		var manager = new InstanceManager();
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var stage = MakeStage();
		var empty = new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new StageInstance { Identity = new InstanceIdentity(stage, empty) };
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1) });
		manager.Add(inst);

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);
		scanner.Tick(DateTimeOffset.UtcNow);

		// Имитируем consumer-цикл, который в реальности делает EventLoop:
		// 1) HandleTimerTick освобождает pendingTick (ReleasePendingTick).
		// 2) BeginIteration: State=Running, NextAutoUtc=null.
		// 3) StageCompleted: State=Idle, новый NextAutoUtc.
		inst.ReleasePendingTick();
		inst.State = InstanceLifecycleState.Running;
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = null });
		while (channel.Reader.TryRead(out _)) { }
		inst.State = InstanceLifecycleState.Idle;
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1) });

		scanner.Tick(DateTimeOffset.UtcNow);

		channel.Reader.TryRead(out var evt).Should().BeTrue("после полного жизненного цикла итерации pendingTick-флаг сброшен → новый tick");
		evt.Should().BeOfType<TimerTickedEvent>();
		scanner.Dispose();
	}
}
