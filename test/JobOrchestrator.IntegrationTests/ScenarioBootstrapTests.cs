using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

public sealed class ScenarioBootstrapTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task SingleKeylessStage_RunsAtBootstrap() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				jobs.Stage("a")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => s.AddSingleton<FakeServiceA>());

		var fake = host.Services.GetRequiredService<FakeServiceA>();
		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fake.WaitForNextCallAsync(Timeout).ConfigureAwait(false)).Should().BeTrue();
			fake.Calls[0].FullyQualifiedName.Should().Be("a[]");
			fake.Calls[0].DependencyKeys.Should().BeEmpty();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task FanOutChain_KeyPropagatesDownGraph() {
		// shops эмитит uuid в keyspace, productGroups создаётся per uuid, products наследует ключи,
		// documents наследует от products. Один AddKey → 3 каскадных инстанса вниз.
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromMinutes(1));
				var pg = jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromMinutes(1));
				var products = jobs.Stage("products")
					.HandledBy<FakeServiceC>()
					.DependsOn(pg)
					.RunPeriodically(TimeSpan.FromMinutes(1));
				jobs.Stage("documents")
					.HandledBy<FakeServiceD>()
					.DependsOn(products)
					.RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
				s.AddSingleton<FakeServiceC>();
				s.AddSingleton<FakeServiceD>();
			});

		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();
		var productsFake = host.Services.GetRequiredService<FakeServiceC>();
		var documentsFake = host.Services.GetRequiredService<FakeServiceD>();

		shopsFake.ExecuteHandler = async (ctx, ct) => {
			await ctx.AddKeyAsync("u1", ct);
		};

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await shopsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("shops bootstrap");
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("productGroups спавнится по AddKey + LastSuccess(shops)");
			(await productsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("products наследует от pg через DependsOn");
			(await documentsFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue("documents наследует от products");

			pgFake.Calls[0].FullyQualifiedName.Should().Be("productGroups[shops=u1]");
			productsFake.Calls[0].FullyQualifiedName.Should().Be("products[shops=u1]");
			documentsFake.Calls[0].FullyQualifiedName.Should().Be("documents[shops=u1]");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task GetOverview_ReturnsAllExistingInstances() {
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var a = jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromMinutes(1));
				jobs.Stage("b").HandledBy<FakeServiceB>().DependsOn(a).RunPeriodically(TimeSpan.FromMinutes(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

		var fakeA = host.Services.GetRequiredService<FakeServiceA>();
		var fakeB = host.Services.GetRequiredService<FakeServiceB>();
		var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await fakeB.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			var overview = orchestrator.GetOverview();
			overview.Instances.Select(j => j.FullyQualifiedName).Should().BeEquivalentTo(["a[]", "b[]"]);
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
