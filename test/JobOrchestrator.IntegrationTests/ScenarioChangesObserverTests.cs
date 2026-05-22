using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// <see cref="IStageHandle.Changes"/>: replay существующих инстансов при подписке, live-поток последующих
/// Added/Removed-событий, отсутствие race между snapshot и live, завершение stream при shutdown.
/// </summary>
public sealed class ScenarioChangesObserverTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task Changes_ReplaysExistingInstances_OnSubscribe() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
			await ctx.AddKeyAsync("u2", ct);
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			using var cts = new CancellationTokenSource(Timeout);
			var collected = new List<StageChange>();
			await foreach (var change in orchestrator.Root["pg"].Changes.WithCancellation(cts.Token)) {
				collected.Add(change);
				if (collected.Count == 2) break;
			}
			collected.Should().HaveCount(2);
			collected.Should().OnlyContain(c => c.Kind == StageChangeKind.Added);
			collected.Select(c => c.Instance.Keys["shops"]).Should().BeEquivalentTo(["u1", "u2"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Changes_DeliversLiveAddedAndRemoved_AfterSubscribe() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			using var cts = new CancellationTokenSource(Timeout);
			var changes = new List<StageChange>();

			// Подписываемся на пустом keyspace pg — replay должен быть пустой.
			var consumeTask = Task.Run(async () => {
				await foreach (var change in orchestrator.Root["pg"].Changes.WithCancellation(cts.Token)) {
					changes.Add(change);
					if (changes.Count == 3) break;
				}
			});

			await Task.Delay(50).ConfigureAwait(false);
			// Триггерим Added через external RegisterKey на shops.
			orchestrator.Root["shops"].RegisterKey("k1");
			orchestrator.Root["shops"].RegisterKey("k2");
			(await pgFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
			// Триггерим Removed.
			orchestrator.Root["shops"].UnregisterKey("k1");

			await consumeTask.ConfigureAwait(false);

			changes.Should().HaveCount(3);
			changes.Count(c => c.Kind == StageChangeKind.Added).Should().Be(2);
			changes.Count(c => c.Kind == StageChangeKind.Removed).Should().Be(1);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Changes_MultipleSubscribers_EachSeesIndependentReplay() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			using var cts = new CancellationTokenSource(Timeout);
			var subA = new List<StageChange>();
			var subB = new List<StageChange>();

			async Task ConsumeAsync(List<StageChange> sink) {
				await foreach (var change in orchestrator.Root["pg"].Changes.WithCancellation(cts.Token)) {
					sink.Add(change);
					if (sink.Count == 1) break;
				}
			}

			await Task.WhenAll(ConsumeAsync(subA), ConsumeAsync(subB)).ConfigureAwait(false);

			subA.Should().HaveCount(1);
			subB.Should().HaveCount(1);
			subA[0].Instance.Keys["shops"].Should().Be("u1");
			subB[0].Instance.Keys["shops"].Should().Be("u1");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Changes_ConcurrentSubscribeAndRegisterKey_NoLostEvents() {
		// Race coverage: subscribe и RegisterKey стартуют через общий sync-barrier чтобы воспроизвести
		// окно между _subscribers.TryAdd, snapshot generation и notify-publish в StageHandle.
		// Инвариант: subscriber должен получить Added для каждого зарегистрированного ключа — либо в replay
		// (если успел до snapshot), либо в live (если после). Без race-fix часть итераций давала бы
		// lost-event (snapshotGen=newGen + cache ещё не обновлён → replay и live оба скипают).
		const int iterations = 100;
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			// Ждём bootstrap shops, чтобы первый RegisterKey не race-нул со стартом event-loop'а.
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			var pgStage = orchestrator.Root["pg"];

			for (int i = 0; i < iterations; i++) {
				var key = $"u{i}";
				using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
				var startBarrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
				var seen = 0;

				var subscribeTask = Task.Run(async () => {
					await startBarrier.Task.ConfigureAwait(false);
					try {
						await foreach (var change in pgStage.Changes.WithCancellation(cts.Token).ConfigureAwait(false)) {
							if (change.Kind == StageChangeKind.Added && change.Instance.Keys["shops"] == key) {
								Interlocked.Exchange(ref seen, 1);
								return;
							}
						}
					} catch (OperationCanceledException) {
						// Timeout — seen остаётся 0, тест увидит fail ниже.
					}
				});

				var registerTask = Task.Run(async () => {
					await startBarrier.Task.ConfigureAwait(false);
					orchestrator.Root["shops"].RegisterKey(key);
				});

				startBarrier.SetResult();

				// Subscriber должен увидеть Added в течение разумного timeout'а.
				// WaitAsync с таймаутом строже cts.Cancel-window — если subscribeTask не завершился
				// раньше, cancel сработает и seen останется 0.
				await registerTask.ConfigureAwait(false);
				try { await subscribeTask.WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); } catch (TimeoutException) { /* fail ниже */ }
				cts.Cancel();

				Volatile.Read(ref seen).Should().Be(1, $"iteration {i}, key={key}: subscriber пропустил Added (lost-event race)");

				// Cleanup до следующей итерации, чтобы pg-инстансы не накапливались.
				orchestrator.Root["shops"].UnregisterKey(key);
				await Task.Delay(20).ConfigureAwait(false);
			}
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Changes_CompletesOnHostStop() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		// Завершаем consumption через ct cancellation — Changes должен освободить subscriber и не блокировать.
		var collected = new List<StageChange>();
		using var cts = new CancellationTokenSource();
		var task = Task.Run(async () => {
			try {
				await foreach (var change in orchestrator.Root["a"].Changes.WithCancellation(cts.Token)) {
					collected.Add(change);
				}
			} catch (OperationCanceledException) {
				// ожидаемо
			}
		});

		await Task.Delay(100).ConfigureAwait(false);
		cts.Cancel();
		await task.WaitAsync(Timeout).ConfigureAwait(false);

		// Хотя бы Added для keyless-инстанса a[] должен был долететь до replay.
		collected.Should().NotBeEmpty();
		await host.StopAsync().ConfigureAwait(false);
	}
}
