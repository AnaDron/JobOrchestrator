using System.Collections.Concurrent;
using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Контракт: события <c>AddKey/RemoveKey</c> от инстанса, помеченного на удаление каскадом,
/// игнорируются оркестратором — нельзя породить orphan-bucket в keyspace и «зомби»-потомка.
/// <para>
/// Regression-coverage для <c>EventLoop.IgnoredAddKeyFromTerminating</c> / <c>IgnoredRemoveKeyFromTerminating</c>:
/// если фильтр уберут, terminating-эмитер сможет создать orphan-инстансы через late AddKey после Cancel.
/// </para>
/// </summary>
public sealed class ScenarioTerminatingFilterTests {
	[Fact]
	public async Task AddKey_FromTerminatingEmitter_IsDiscarded() {
		var mid = new MidEmitterStage();
		var leaf = new LeafCounterStage();

		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Warning);
		b.Services.AddSingleton<RootStage>();
		b.Services.AddSingleton(mid);
		b.Services.AddSingleton(leaf);
		b.Services.AddJobOrchestrator(jobs => {
			jobs.UseInMemoryStateStore();
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromSeconds(30));

			var root = jobs.Stage("root")
				.HandledBy<RootStage>()
				.RunPeriodically(TimeSpan.FromMinutes(1));
			var midStage = jobs.Stage("mid")
				.HandledBy<MidEmitterStage>()
				.DependsOnInstance(root)
				.RunPeriodically(TimeSpan.FromMinutes(1));
			jobs.Stage("leaf")
				.HandledBy<LeafCounterStage>()
				.DependsOnInstance(midStage)
				.RunPeriodically(TimeSpan.FromMinutes(1));
		});

		using var host = b.Build();
		await host.StartAsync().ConfigureAwait(false);
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		try {
			// Дождаться bootstrap keyless-инстанса root[] через event-loop — иначе RegisterKey
			// бросает «keyless-инстанс ещё не создан» (race с async StartAsync → background-loop).
			await AsyncWait.UntilAsync(() => orchestrator["root"][InstanceKey.None].Snapshot is not null,
				TimeSpan.FromSeconds(5), "root[] должен пробуститься event-loop'ом");
			orchestrator["root"].RegisterKey("r1");

			await AsyncWait.UntilAsync(() => leaf.RunsForB("b1") >= 1, TimeSpan.FromSeconds(5),
				"leaf[root=r1,mid=b1] должен стартовать после AddKey(\"b1\") в mid");

			// На этой точке mid висит на gate. Каскадно отменяем mid[root=r1] и потомков.
			orchestrator["root"].UnregisterKey("r1");

			// Дать каскаду пометить mid в Terminating и Cancel-нуть его CTS.
			await AsyncWait.UntilAsync(() => mid.SeenCancellation, TimeSpan.FromSeconds(5),
				"mid должен увидеть Cancel от каскада");

			// Отпускаем mid — после Cancel он попытается опубликовать AddKey("b2").
			// Это событие должно быть отброшено фильтром terminating.
			mid.ReleaseGate();

			// Ждём, пока все аффектированные инстансы исчезнут (остаётся только root[]).
			await AsyncWait.UntilAsync(() => {
				var snap = orchestrator.GetOverview();
				return snap.Instances.All(j => j.StageName == "root");
			}, TimeSpan.FromSeconds(5), "после каскадного удаления должен остаться только root[]");

			// Главный assert: leaf[mid=b2] НЕ должен был быть создан.
			leaf.RunsForB("b2").Should().Be(0,
				"AddKey(\"b2\") после Cancel — событие от terminating-эмитера, должно быть отброшено");

			var overview = orchestrator.GetOverview();
			overview.Instances.Should().NotContain(j => j.StageName == "leaf");
			overview.Instances.Should().NotContain(j => j.StageName == "mid");
		} finally {
			mid.ReleaseGate();
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}

	internal sealed class RootStage : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	internal sealed class MidEmitterStage : IJobService {
		private readonly TaskCompletionSource _gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
		private int _seenCancellation;

		public bool SeenCancellation => Volatile.Read(ref _seenCancellation) == 1;

		public void ReleaseGate() => _gate.TrySetResult();

		public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
			await ctx.AddKeyAsync("b1", ct);
			try {
				await _gate.Task.WaitAsync(ct).ConfigureAwait(false);
			} catch (OperationCanceledException) {
				Volatile.Write(ref _seenCancellation, 1);
				// «Плохой гражданин»: после Cancel зовёт AddKey. SDK должен это отбросить,
				// потому что инстанс уже помечен на удаление.
				try {
					await ctx.AddKeyAsync("b2", CancellationToken.None);
				} catch (InvalidOperationException) {
					// Channel уже закрыт — тоже допустимо при гонке со shutdown.
				}
				throw;
			}
		}
	}

	internal sealed class LeafCounterStage : IJobService {
		private readonly ConcurrentDictionary<string, int> _runs = new();

		public int RunsForB(string b) => _runs.TryGetValue(b, out var n) ? n : 0;

		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
			var mid = ctx.DependencyKeys["mid"];
			_runs.AddOrUpdate(mid, 1, (_, v) => v + 1);
			return Task.CompletedTask;
		}
	}
}
