using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioConcurrentStressTests {
	[Fact]
	public async Task ParallelTriggerRegisterUnregisterOverview_NoDeadlockNoCrash() {
		// 8 worker-потоков параллельно дёргают разные методы IJobOrchestrator из внешнего кода
		// в течение фиксированного бюджета времени. Проверяем: нет deadlock-а (всё завершается
		// в бюджет), нет крах (IsFaulted остаётся false), GetOverview после стресса возвращает
		// консистентный snapshot.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var producer = jobs.Stage("producer")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
				jobs.Stage("consumer")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(producer)
					.RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

		await host.StartAsync().ConfigureAwait(false);
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		// Бюджет — реалистичная нагрузка: 2 секунды × 8 потоков × ~50 ops/poток = ~800 операций.
		using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
		try {
			var tasks = new List<Task>();
			for (int w = 0; w < 8; w++) {
				int worker = w;
				tasks.Add(Task.Run(async () => {
					for (int j = 0; !stopCts.IsCancellationRequested && j < 200; j++) {
						string key = $"w{worker}-k{j % 10}";
						try {
							switch (j % 4) {
								case 0: orchestrator["producer"].RegisterKey(key); break;
								case 1: await orchestrator["producer"][InstanceKey.None].RunAsync(stopCts.Token).ConfigureAwait(false); break;
								case 2: orchestrator["producer"].UnregisterKey(key); break;
								case 3: orchestrator.GetOverview(); break;
							}
						} catch (OperationCanceledException) {
							break;
						} catch (InvalidOperationException) {
							// Может прилететь если IsFaulted был выставлен между check и call — для теста OK.
							break;
						} catch (IterationRejectedException) {
							// Stress-loop: Debounced/AlreadyRunning/ConcurrencyDeferred — ожидаемые отказы.
							continue;
						}
					}
				}, stopCts.Token));
			}

			// WhenAny с финальным таймаутом — гарантия отсутствия deadlock-а.
			var allDone = Task.WhenAll(tasks);
			var deadlineMargin = Task.Delay(TimeSpan.FromSeconds(5));
			var winner = await Task.WhenAny(allDone, deadlineMargin).ConfigureAwait(false);
			winner.Should().BeSameAs(allDone, "все worker-потоки должны завершиться в бюджет — иначе deadlock");

			orchestrator.IsFaulted.Should().BeFalse("стресс не должен крашить event loop");

			// Финальный smoke: GetOverview по-прежнему отвечает консистентным snapshot-ом.
			var finalOverview = orchestrator.GetOverview();
			finalOverview.Instances.Should().NotBeNull();
			// producer-инстанс точно существует (создан в bootstrap, не удалялся).
			finalOverview.Instances.Should().Contain(i => i.StageName == "producer" && i.FullyQualifiedName == "producer[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task ParallelRegisterUnregisterSameKey_FinalKeyspaceConsistent() {
		// Несколько worker-потоков параллельно делают RegisterKey/UnregisterKey по одинаковым ключам.
		// Финальное состояние должно быть детерминированным: ни один консьюмер-инстанс не "утёк",
		// keyspace consistency сохраняется.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var producer = jobs.Stage("producer")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
				jobs.Stage("consumer")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(producer)
					.RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

		await host.StartAsync().ConfigureAwait(false);
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
		var producerFake = host.Services.GetRequiredService<FakeServiceA>();
		(await producerFake.WaitForCallCountAsync(1, TimeSpan.FromSeconds(5)).ConfigureAwait(false)).Should().BeTrue();

		// Все потоки оперируют одним keyspace {"a", "b", "c"} в случайном порядке.
		var keys = new[] { "a", "b", "c" };
		var tasks = new List<Task>();
		for (int w = 0; w < 4; w++) {
			tasks.Add(Task.Run(() => {
				for (int j = 0; j < 50; j++) {
					string k = keys[j % 3];
					if (j % 2 == 0) orchestrator["producer"].RegisterKey(k);
					else orchestrator["producer"].UnregisterKey(k);
				}
			}));
		}
		await Task.WhenAll(tasks).ConfigureAwait(false);
		// Дать event loop'у обработать всю очередь событий.
		await Task.Delay(500).ConfigureAwait(false);

		orchestrator.IsFaulted.Should().BeFalse();

		// Финальное состояние keyspace недетерминированно (Register/Unregister в случайном порядке),
		// НО оно должно быть консистентным: для каждого consumer-инстанса в overview его ключ
		// должен быть в keyspace producer'а (через косвенный признак — finalCount instances <= 3).
		var overview = orchestrator.GetOverview();
		var consumerInstances = overview.Instances.Where(i => i.StageName == "consumer").ToList();
		consumerInstances.Should().HaveCountLessThanOrEqualTo(3, "consumer не может иметь больше инстансов, чем размер keyspace");
		consumerInstances.Select(i => i.DependencyKeys["producer"]).Should().OnlyContain(k => keys.Contains(k));

		await host.StopAsync().ConfigureAwait(false);
	}
}
