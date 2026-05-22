using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// <see cref="IInstanceHandle"/>-as-<see cref="IAsyncEnumerable{IIterationHandle}"/>: поток итераций,
/// <see cref="IInstanceHandle.RunningIteration"/> отражает активную итерацию или null, stream завершается
/// при cascade-removal.
/// </summary>
public sealed class ScenarioInstanceIterationStreamTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task IterationStream_EmitsRunIterations() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("первая авто-итерация");
			await Task.Delay(100).ConfigureAwait(false);

			var handle = orchestrator.Root["a"][InstanceKeys.Empty];
			using var cts = new CancellationTokenSource(Timeout);
			var captured = new List<IIterationHandle>();
			var consume = Task.Run(async () => {
				await foreach (var iter in handle.WithCancellation(cts.Token)) {
					captured.Add(iter);
					if (captured.Count == 2) break;
				}
			});

			// Триггерим 2 manual-итерации последовательно.
			await Task.Delay(50).ConfigureAwait(false);
			var i1 = await handle.RunAsync(cts.Token).ConfigureAwait(false);
			await i1.Completion.ConfigureAwait(false);
			var i2 = await handle.RunAsync(cts.Token).ConfigureAwait(false);
			await i2.Completion.ConfigureAwait(false);

			await consume.ConfigureAwait(false);

			captured.Should().HaveCount(2);
			captured.Should().OnlyContain(i => i.FullyQualifiedName == "a[]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RunningIteration_ReflectsActiveIteration_OrNull() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var enter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		fake.ExecuteHandler = async (ctx, ct) => {
			enter.TrySetResult();
			await gate.Task.ConfigureAwait(false);
		};
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var handle = orchestrator.Root["a"][InstanceKeys.Empty];

			await enter.Task.WaitAsync(Timeout).ConfigureAwait(false);
			handle.RunningIteration.Should().NotBeNull("итерация активна (gate ещё не отпущен)");
			handle.RunningIteration!.FullyQualifiedName.Should().Be("a[]");

			gate.SetResult();
			await AsyncWait.UntilAsync(() => handle.RunningIteration is null, Timeout,
				"после completion RunningIteration сбрасывается в null").ConfigureAwait(false);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task IterationStream_CompletesOnCascadeRemoval() {
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
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			// Ждём bootstrap shops-инстанса перед RegisterKey — иначе race с event-loop startup.
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			orchestrator.Root["shops"].RegisterKey("u1");
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var handle = orchestrator.Root["pg"][("shops", "u1")];
			using var cts = new CancellationTokenSource(Timeout);
			var streamDone = Task.Run(async () => {
				await foreach (var _ in handle.WithCancellation(cts.Token)) { /* потребляем до Completion */ }
			});

			await Task.Delay(50).ConfigureAwait(false);
			orchestrator.Root["shops"].UnregisterKey("u1");

			await streamDone.WaitAsync(Timeout).ConfigureAwait(false);
			handle.State.Should().BeNull("инстанс удалён каскадом");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
