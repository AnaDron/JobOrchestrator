using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Сценарии для handle-API (<c>orchestrator["x"][...]</c>): валидирует, что новый surface даёт ту же
/// семантику, что и flat-API, плюс уникальные операции (State, Snapshot, AllInstances).
/// </summary>
public sealed class ScenarioHandleApiTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task Indexer_KnownStage_ReturnsHandle() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var handle = orchestrator["a"];
			handle.Should().NotBeNull();
			handle.Name.Should().Be("a");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_UnknownStage_Throws() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			Action act = () => _ = orchestrator["nonexistent"];
			act.Should().Throw<ArgumentException>().WithMessage("*nonexistent*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_SameStageHandle_Cached() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var h1 = orchestrator["a"];
			var h2 = orchestrator["a"];
			ReferenceEquals(h1, h2).Should().BeTrue("StageHandle cached в Runtime");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task InstanceKey_None_KeylessTrigger_Works() {
		// orchestrator["a"][InstanceKey.None].TriggerAsync() — keyless через handle-API.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("auto-tick");

			var result = await orchestrator["a"][InstanceKey.None].TriggerAsync().ConfigureAwait(false);
			result.Should().Be(TriggerResult.Started);
			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task SingleKey_Indexer_Works() {
		// orchestrator["pg"][("shops","u1")].TriggerAsync() — 1-key через tuple-indexer.
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
		shopsFake.ExecuteHandler = (ctx, _) => {
			ctx.AddKey("u1");
			return Task.CompletedTask;
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();

			var result = await orchestrator["pg"][("shops", "u1")].TriggerAsync().ConfigureAwait(false);
			result.Should().Be(TriggerResult.Started);
			(await pgFake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task State_KeylessInstance_ReflectsLifecycle() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var state = orchestrator["a"][InstanceKey.None].State;
			state.Should().Be(InstanceLifecycleState.Idle, "после first iteration инстанс Idle");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task State_NonexistentKeyedInstance_ReturnsNull() {
		// Для keyed-стадии без emitted-ключей инстансов нет → State == null.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			// shops запускается, но keys не эмитит — pg-инстанса с ("shops","u1") нет.
			var state = orchestrator["pg"][("shops", "u1")].State;
			state.Should().BeNull("инстанс не материализован");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task RegisterKey_OnStageHandle_PropagatesToCascade() {
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
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();

			// External bootstrap через handle-API.
			orchestrator["shops"].RegisterKey("u-ext");
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("pg-инстанс с emitted key");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task WaitForSuccessAsync_OnInstanceHandle_Resolves() {
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			await orchestrator["a"][InstanceKey.None]
				.WaitForSuccessAsync(new CancellationTokenSource(Timeout).Token)
				.ConfigureAwait(false);
			// Достижение этой точки — success.
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task AllInstances_AfterEmitting_ReflectsAllEmittedKeys() {
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
		shopsFake.ExecuteHandler = (ctx, _) => {
			ctx.AddKey("u1");
			ctx.AddKey("u2");
			ctx.AddKey("u3");
			return Task.CompletedTask;
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(3, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var instances = orchestrator["pg"].AllInstances;
			instances.Should().HaveCount(3);
			instances.Select(i => i.DependencyKeys["shops"]).Should().BeEquivalentTo(["u1", "u2", "u3"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
