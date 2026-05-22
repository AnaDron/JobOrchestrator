using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Сценарии для handle-API (<c>orchestrator.Root["x"][...]</c>): валидирует, что новый surface даёт ту же
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
			var handle = orchestrator.Root["a"];
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
			Action act = () => _ = orchestrator.Root["nonexistent"];
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
			var h1 = orchestrator.Root["a"];
			var h2 = orchestrator.Root["a"];
			ReferenceEquals(h1, h2).Should().BeTrue("StageHandle cached в Runtime");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task InstanceKey_None_KeylessRunAsync_Works() {
		// orchestrator.Root["a"][InstanceKeys.Empty].RunAsync() — keyless через handle-API.
		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());
		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await fake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("auto-tick");
			await Task.Delay(100).ConfigureAwait(false);

			var iteration = await orchestrator.Root["a"][InstanceKeys.Empty].RunAsync().ConfigureAwait(false);
			iteration.Should().NotBeNull();
			(await fake.WaitForCallCountAsync(2, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task SingleKey_Indexer_Works() {
		// orchestrator.Root["pg"][("shops","u1")].RunAsync() — 1-key через tuple-indexer.
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

			var iteration = await orchestrator.Root["pg"][("shops", "u1")].RunAsync().ConfigureAwait(false);
			iteration.Should().NotBeNull();
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

			var state = orchestrator.Root["a"][InstanceKeys.Empty].State;
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
			var state = orchestrator.Root["pg"][("shops", "u1")].State;
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
			orchestrator.Root["shops"].RegisterKey("u-ext");
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
			await orchestrator.Root["a"][InstanceKeys.Empty]
				.WaitForSuccessAsync(new CancellationTokenSource(Timeout).Token)
				.ConfigureAwait(false);
			// Достижение этой точки — success.
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_InvalidKeyName_ThrowsArgumentException() {
		// orchestrator.Root["pg"][("wrong-key", "v")] → fail-fast в момент handle-construction,
		// а не silent-NotFound в RunAsync.
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
			Action act = () => _ = orchestrator.Root["pg"][("nonexistent-key", "v")];
			act.Should().Throw<ArgumentException>().WithMessage("*nonexistent-key*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_WrongKeyCount_ThrowsArgumentException() {
		// pg ожидает 1 key (shops); передаём 2 → fail-fast.
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
			Action act = () => _ = orchestrator.Root["pg"][new InstanceKeys(("shops", "u1"), ("extra", "v"))];
			act.Should().Throw<ArgumentException>().WithMessage("*ожидает 1*передано 2*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Indexer_KeylessOnKeyedStage_ThrowsArgumentException() {
		// orchestrator.Root["pg"][InstanceKeys.Empty] на стадии с зависимостями → fail-fast.
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
			Action act = () => _ = orchestrator.Root["pg"][InstanceKeys.Empty];
			act.Should().Throw<ArgumentException>().WithMessage("*ожидает 1*передано 0*");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task IJobOrchestrator_AllStages_ReturnsFlatStageList() {
		// foreach (var stage in orchestrator) теперь даёт IDomainHandle. Плоский срез по стадиям —
		// через extension orchestrator.AllStages() либо orchestrator.Root для бездоменных.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("b").HandledBy<FakeServiceB>().RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			var names = orchestrator.AllStages().Select(s => s.Name).OrderBy(n => n).ToArray();
			names.Should().BeEquivalentTo(["a", "b"]);
			// foreach по orchestrator — это домены; для бездоменных только root.
			orchestrator.Should().HaveCount(1, because: "только root-домен (нет .WithDomain)");
			orchestrator.Root.Select(s => s.Name).Should().BeEquivalentTo(["a", "b"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task InstanceHandle_Equality_BasedOnIdentity() {
		// Два handle на один и тот же логический инстанс → value-equality.
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
			var h1 = orchestrator.Root["pg"][("shops", "u1")];
			var h2 = orchestrator.Root["pg"][("shops", "u1")];
			var h3 = orchestrator.Root["pg"][("shops", "u2")];

			h1.Equals(h2).Should().BeTrue("одинаковая Identity → value-equality");
			h1.GetHashCode().Should().Be(h2.GetHashCode());
			h1.Equals(h3).Should().BeFalse("разные keys");

			// Также проверим использование в HashSet — типовой scenario.
			var set = new HashSet<IInstanceHandle>([h1, h2, h3]);
			set.Should().HaveCount(2, "h1 ≡ h2, h3 — отдельный");
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
		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
			await ctx.AddKeyAsync("u2", ct);
			await ctx.AddKeyAsync("u3", ct);
		};
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			(await pgFake.WaitForCallCountAsync(3, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await Task.Delay(100).ConfigureAwait(false);

			var instances = orchestrator.Root["pg"];
			instances.Should().HaveCount(3);
			instances.Select(i => i.Keys["shops"]).Should().BeEquivalentTo(["u1", "u2", "u3"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
