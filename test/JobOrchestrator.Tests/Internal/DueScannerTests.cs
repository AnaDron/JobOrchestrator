using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;

namespace JobOrchestrator.Tests.Internal;

public sealed class DueScannerTests {
	private static StageDescriptor MakeStage(string name) => TestStages.Make(name);

	private static Instance MakeInstance(StageDescriptor stage, DateTimeOffset? nextAuto, InstanceLifecycleState state = InstanceLifecycleState.Idle) {
		var empty = new Dictionary<string, string>(StringComparer.Ordinal);
		var inst = new Instance { Identity = new InstanceIdentity(stage, empty) };
		inst.SetMetrics(inst.Metrics with { NextAutoUtc = nextAuto });
		if (state == InstanceLifecycleState.Running) inst.TryBeginRunning();
		else if (state == InstanceLifecycleState.Terminating) inst.MarkTerminating();
		return inst;
	}

	[Fact]
	public async Task ScansAndPublishes_DueIdleInstances() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var manager = new InstanceManager();
		var stage = MakeStage("x");

		// Три инстанса с разными "marker"-ключами, чтобы Identity-equality их различал в InstanceManager:
		// один уже due, один — в будущем, один — Running (должен быть пропущен).
		Instance MakeMarked(string marker, DateTimeOffset nextAuto, InstanceLifecycleState state = InstanceLifecycleState.Idle) {
			var keys = new Dictionary<string, string>(StringComparer.Ordinal) { ["marker"] = marker };
			var inst = new Instance { Identity = new InstanceIdentity(stage, keys) };
			inst.SetMetrics(inst.Metrics with { NextAutoUtc = nextAuto });
			if (state == InstanceLifecycleState.Running) inst.TryBeginRunning();
			else if (state == InstanceLifecycleState.Terminating) inst.MarkTerminating();
			return inst;
		}
		var dueInstance = MakeMarked("due", DateTimeOffset.UtcNow.AddMilliseconds(-10));
		manager.Add(dueInstance);
		var futureInstance = MakeMarked("future", DateTimeOffset.UtcNow.AddMinutes(5));
		manager.Add(futureInstance);
		var runningInstance = MakeMarked("running", DateTimeOffset.UtcNow.AddMilliseconds(-10), InstanceLifecycleState.Running);
		manager.Add(runningInstance);

		var scanner = new DueScanner(manager, channel, TimeProvider.System, NullLogger<DueScanner>.Instance);
		using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		var loop = scanner.RunAsync(cts.Token);

		// Дать одну итерацию scanner-а пройти, разбудить и завершить.
		await Task.Delay(100, cts.Token).ConfigureAwait(false);
		scanner.Wake();
		cts.Cancel();
		try { await loop.ConfigureAwait(false); } catch (OperationCanceledException) { }

		var published = new List<OrchestratorEvent>();
		while (channel.Reader.TryRead(out var evt)) published.Add(evt);

		// Опубликован тик именно для due-инстанса; running и future-инстансы — пропущены.
		published.OfType<TimerTickedEvent>().Select(e => e.Instance.FullyQualifiedName)
			.Should().Contain("x[marker=due]")
			.And.NotContain("x[marker=future]")
			.And.NotContain("x[marker=running]");
	}

	[Fact]
	public async Task Wake_InterruptsLongSleep() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var manager = new InstanceManager();
		var stage = MakeStage("x");

		// Всё в далёком будущем — без Wake() scanner спал бы дольше теста.
		var farFuture = new Instance { Identity = new InstanceIdentity(stage) };
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

		channel.Reader.TryRead(out var evt).Should().BeTrue();
		evt.Should().BeOfType<TimerTickedEvent>()
			.Which.Instance.FullyQualifiedName.Should().Be("x[]");
	}
}
