using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;

namespace JobOrchestrator.Tests.Internal;

public sealed class DueScannerTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name) => new() {
		Name = name,
		ServiceType = typeof(FakeService),
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = [],
	};

	private static StageInstance MakeInstance(StageDescriptor stage, DateTimeOffset? nextAuto, InstanceLifecycleState state = InstanceLifecycleState.Idle) {
		var empty = new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new StageInstance {
			Stage = stage,
			DependencyKeys = empty,
			FullyQualifiedName = DependencyKey.FormatFullyQualifiedName(stage.Name, empty),
			EncodedKey = DependencyKey.Encode(empty),
		};
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = nextAuto });
		inst.State = state;
		return inst;
	}

	[Fact]
	public async Task ScansAndPublishes_DueIdleInstances() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var manager = new InstanceManager();
		var stage = MakeStage("x");

		// Один идле-инстанс уже due, один — в будущем, один — Running (должен быть пропущен).
		var dueInstance = MakeInstance(stage, DateTimeOffset.UtcNow.AddMilliseconds(-10));
		dueInstance = new StageInstance { Stage = stage, DependencyKeys = dueInstance.DependencyKeys, FullyQualifiedName = "x[due]", EncodedKey = "due" };
		dueInstance.SetMetrics(dueInstance.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-10) });
		manager.Add(dueInstance);

		var futureInstance = new StageInstance { Stage = stage, DependencyKeys = dueInstance.DependencyKeys, FullyQualifiedName = "x[future]", EncodedKey = "future" };
		futureInstance.SetMetrics(futureInstance.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMinutes(5) });
		manager.Add(futureInstance);

		var runningInstance = new StageInstance { Stage = stage, DependencyKeys = dueInstance.DependencyKeys, FullyQualifiedName = "x[running]", EncodedKey = "running" };
		runningInstance.SetMetrics(runningInstance.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-10) });
		runningInstance.State = InstanceLifecycleState.Running;
		manager.Add(runningInstance);

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);
		using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var loop = scanner.RunAsync(cts.Token);

		// Дать одну итерацию scanner-а пройти, разбудить и завершить.
		await Task.Delay(100, cts.Token).ConfigureAwait(false);
		scanner.Wake();
		cts.Cancel();
		try { await loop.ConfigureAwait(false); } catch (OperationCanceledException) { }
		scanner.Dispose();

		var published = new List<OrchestratorEvent>();
		while (channel.Reader.TryRead(out var evt)) published.Add(evt);

		// Опубликован тик именно для due-инстанса; running и future-инстансы — пропущены.
		published.OfType<TimerTickedEvent>().Select(e => e.Instance.FullyQualifiedName)
			.Should().Contain("x[due]")
			.And.NotContain("x[future]")
			.And.NotContain("x[running]");
	}

	[Fact]
	public async Task Wake_InterruptsLongSleep() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var manager = new InstanceManager();
		var stage = MakeStage("x");

		// Всё в далёком будущем — без Wake() scanner спал бы дольше теста.
		var farFuture = new StageInstance {
			Stage = stage,
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[far]",
			EncodedKey = "far",
		};
		farFuture.SetMetrics(farFuture.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMinutes(10) });
		manager.Add(farFuture);

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);
		using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var loop = scanner.RunAsync(cts.Token);

		await Task.Delay(100, cts.Token).ConfigureAwait(false);
		// Меняем NextAutoUtc и будим — сканер должен сразу пересчитать и опубликовать tick.
		farFuture.SetMetrics(farFuture.Metrics with { NextAutoUtc = DateTimeOffset.UtcNow.AddMilliseconds(-1) });
		scanner.Wake();

		// Подождём публикацию.
		var sw = System.Diagnostics.Stopwatch.StartNew();
		while (sw.Elapsed < TimeSpan.FromMilliseconds(500)) {
			if (channel.Reader.TryPeek(out _)) break;
			await Task.Delay(20, cts.Token).ConfigureAwait(false);
		}

		cts.Cancel();
		try { await loop.ConfigureAwait(false); } catch (OperationCanceledException) { }
		scanner.Dispose();

		channel.Reader.TryRead(out var evt).Should().BeTrue();
		evt.Should().BeOfType<TimerTickedEvent>()
			.Which.Instance.FullyQualifiedName.Should().Be("x[far]");
	}
}
