using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// 3.4: после рестарта оркестратор bootstrap'нется с чистым in-memory state — LastSuccess=null
/// у всех инстансов, первая итерация после рестарта = «first success» → cascade всем зависимым.
/// </summary>
public sealed class ScenarioRestartBootstrapTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task Restart_ResetsInMemoryStateButPreservesIJobStateStore() {
		// Phase 1: первый запуск — shops эмитит ключ, dependent создаётся.
		var shopsFake1 = new FakeServiceA();
		shopsFake1.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("shop-1", ct); };
		var pgFake1 = new FakeServiceB();
		int phaseCalls1;

		using (var host1 = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake1);
				s.AddSingleton<FakeServiceB>(pgFake1);
			})) {
			await host1.StartAsync().ConfigureAwait(false);
			try {
				(await pgFake1.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
				phaseCalls1 = pgFake1.CallCount;
			} finally {
				await host1.StopAsync().ConfigureAwait(false);
			}
		}
		phaseCalls1.Should().Be(1, "первый запуск создал и выполнил один pg-инстанс");

		// Phase 2: новый host, тот же граф. In-memory state СБРОСИЛСЯ — bootstrap пройдёт заново,
		// shops снова получит первую итерацию → AddKey → pg снова bootstrap'нется.
		var shopsFake2 = new FakeServiceA();
		shopsFake2.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("shop-1", ct); };
		var pgFake2 = new FakeServiceB();

		using (var host2 = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops")
					.HandledBy<FakeServiceA>()
					.RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("productGroups")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake2);
				s.AddSingleton<FakeServiceB>(pgFake2);
			})) {
			await host2.StartAsync().ConfigureAwait(false);
			try {
				// Второй host видит pg-вызов как «первый success» (LastSuccess null после рестарта).
				(await pgFake2.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
				// LastSuccessAt в JobContext должен быть null на первой итерации (in-memory bootstrap).
				pgFake2.Calls.Should().NotBeEmpty();
				pgFake2.Calls[0].LastSuccessAt.Should().BeNull(
					"после рестарта LastSuccessAt сбрасывается — БЛ может различать «первый запуск» (full sync) от последующих (delta)");
			} finally {
				await host2.StopAsync().ConfigureAwait(false);
			}
		}
	}
}
