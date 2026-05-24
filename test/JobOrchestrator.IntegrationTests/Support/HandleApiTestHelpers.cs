using System.Diagnostics;

namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Общие helpers для handle-API сценариев: фабрики двух чаще всего используемых графов
/// и явное ожидание «инстанс отработал bootstrap-tick и снова Idle» вместо магической
/// <c>Task.Delay(100)</c>-паузы.
/// </summary>
internal static class HandleApiTestHelpers {
	public static IHost BuildKeylessAHost() =>
		TestHostFactory.Build(
			configure: jobs => jobs.Stage("a").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1)),
			registerFakes: s => s.AddSingleton<FakeServiceA>());

	public static IHost BuildShopsPgHost() =>
		TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg").HandledBy<FakeServiceB>().DependsOnInstance(shops).RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>();
				s.AddSingleton<FakeServiceB>();
			});

	/// <summary>
	/// Ждать пока инстанс снова Idle И у него зафиксирован хотя бы один LastSuccess.
	/// Замена для <c>await Task.Delay(100)</c>-bandage: явно опрашиваем целевое состояние,
	/// а не «угадываем» сколько времени event-loop'у нужно после завершения runner'а.
	/// </summary>
	public static async Task WaitForFirstIdleAsync(IInstanceHandle handle, TimeSpan timeout) {
		var sw = Stopwatch.StartNew();
		while (sw.Elapsed < timeout) {
			var snapshot = handle.Snapshot;
			if (snapshot is not null
				&& handle.State == InstanceLifecycleState.Idle
				&& snapshot.LastSuccess is not null) {
				return;
			}
			await Task.Delay(20).ConfigureAwait(false);
		}
		throw new TimeoutException(
			$"Инстанс {handle.FullyQualifiedName} не вышел в Idle с LastSuccess за {timeout}; " +
			$"текущий state={handle.State}, snapshot={handle.Snapshot}.");
	}
}
