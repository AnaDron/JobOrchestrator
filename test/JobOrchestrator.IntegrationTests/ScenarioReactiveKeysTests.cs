using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Контракт: <c>ctx.AddKey/RemoveKey</c> реактивны — событие публикуется немедленно, не дожидаясь
/// завершения текущего <c>ExecuteAsync</c>. Зависимый инстанс создаётся и стартует, пока эмитер
/// ещё внутри <c>ExecuteAsync</c>.
/// <para>
/// Regression-coverage: если кто-то заменит синхронную <c>Channel.Writer.Publish(KeyAddedEvent)</c>
/// на batched-after-success flush — мы потеряем reactivity → этот тест упадёт.
/// </para>
/// </summary>
public sealed class ScenarioReactiveKeysTests {
	[Fact]
	public async Task AddKey_PublishedImmediately_DependentRunsBeforeEmitterFinishes() {
		var emitter = new SlowEmitterStage();
		var child = new ChildCounterStage();

		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Warning);
		b.Services.AddSingleton(emitter);
		b.Services.AddSingleton(child);
		b.Services.AddInMemoryJobStateStore();
		b.Services.AddJobOrchestrator(jobs => {
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromSeconds(30));

			var shops = jobs.Stage("shops")
				.HandledBy<SlowEmitterStage>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			jobs.Stage("groups")
				.HandledBy<ChildCounterStage>()
				.DependsOnInstance(shops)
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
		});

		using var host = b.Build();
		await host.StartAsync().ConfigureAwait(false);

		try {
			// Главный assert: child СТАРТОВАЛ, пока emitter ещё inside ExecuteAsync.
			await AsyncWait.UntilAsync(() => child.Runs >= 1, TimeSpan.FromSeconds(5),
				"groups[shops=s-1] должен стартовать ДО того, как эмитер shops завершит ExecuteAsync");

			emitter.IsStillRunning.Should().BeTrue(
				"эмитер всё ещё внутри ExecuteAsync — AddKey виден оркестратору без after-success-flush");

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			orchestrator["groups"][("shops", "s-1")].State
				.Should().NotBeNull("groups-инстанс материализован реактивно");
		} finally {
			emitter.Release();
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}

	internal sealed class SlowEmitterStage : IJobService {
		private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
		private int _entered;

		public bool IsStillRunning => Volatile.Read(ref _entered) == 1 && !_gate.Task.IsCompleted;

		public void Release() => _gate.TrySetResult();

		public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
			if (Interlocked.CompareExchange(ref _entered, 1, 0) != 0) return;
			await ctx.AddKeyAsync("s-1", ct);
			using var reg = ct.Register(() => _gate.TrySetCanceled(ct));
			await _gate.Task.ConfigureAwait(false);
		}
	}

	internal sealed class ChildCounterStage : IJobService {
		private int _runs;
		public int Runs => Volatile.Read(ref _runs);

		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
			Interlocked.Increment(ref _runs);
			return Task.CompletedTask;
		}
	}
}
