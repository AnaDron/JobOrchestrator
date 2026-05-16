using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Контракт: <c>DependsOn</c>-зависимость остаётся transactional даже когда у стадии параллельно
/// satisfied <c>DependsOnInstance</c>. Reactive-семантика DependsOnInstance НЕ распространяется
/// на DependsOn — child не создаётся пока DependsOn-родитель не имеет LastSuccess.
/// <para>
/// Regression-coverage для гипотетического regression-а «после reactive-DependsOnInstance я могу
/// случайно убрать LastSuccess-проверку и для DependsOn». Не должно быть безусловного создания
/// после AddKey — DependsOn-канал блокирует, пока его родитель не отработал успешно.
/// </para>
/// </summary>
public sealed class ScenarioMixedDependsTests {
	[Fact]
	public async Task Groups_NotCreated_UntilEmployees_HasLastSuccess() {
		// Topology:
		//   shops          (keyless) — эмитит "u-1" сразу
		//   employees      (keyless, gated) — НЕ завершает ExecuteAsync пока тест не release-нет
		//   groups         DependsOnInstance(shops) + DependsOn(employees)
		//
		// Ожидание: после shops.AddKey("u-1") DependsOnInstance(shops) satisfied (reactive),
		// но DependsOn(employees) блокирует — employees не имел LastSuccess. groups не создаётся.
		// После release employees-gate → employees.LastSuccess set → first-success-cascade
		// создаёт groups[shops=u-1].

		var shopsRuns = 0;
		var employeesGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var employeesRuns = 0;
		var groupsRuns = 0;

		var shops = new GatedStage(async (ctx, ct) => {
			Interlocked.Increment(ref shopsRuns);
			if (shopsRuns == 1) await ctx.AddKeyAsync("u-1", ct);  // только первый раз
		});

		var employees = new GatedStage(async (_, ct) => {
			if (Interlocked.Increment(ref employeesRuns) == 1) {
				// Первый запуск — ждём gate.
				using var reg = ct.Register(() => employeesGate.TrySetCanceled(ct));
				await employeesGate.Task.ConfigureAwait(false);
			}
		});

		var groups = new GatedStage((_, _) => {
			Interlocked.Increment(ref groupsRuns);
			return Task.CompletedTask;
		});

		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Warning);
		b.Services.AddSingleton<ShopsStub>(_ => new ShopsStub(shops));
		b.Services.AddSingleton<EmployeesStub>(_ => new EmployeesStub(employees));
		b.Services.AddSingleton<GroupsStub>(_ => new GroupsStub(groups));
		b.Services.AddInMemoryJobStateStore();
		b.Services.AddJobOrchestrator(jobs => {
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromSeconds(30));

			var shopsStage = jobs.Stage("shops")
				.HandledBy<ShopsStub>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			var employeesStage = jobs.Stage("employees")
				.HandledBy<EmployeesStub>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			jobs.Stage("groups")
				.HandledBy<GroupsStub>()
				.DependsOnInstance(shopsStage)
				.DependsOn(employeesStage)
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
		});

		using var host = b.Build();
		await host.StartAsync().ConfigureAwait(false);

		try {
			// Phase 1: shops запустился и эмитнул, но employees заблокирован на gate.
			await AsyncWait.UntilAsync(() => Volatile.Read(ref shopsRuns) >= 1, TimeSpan.FromSeconds(5),
				"shops должен пробуститься и эмитить AddKey");

			// Даём event-loop'у обработать KeyAddedEvent и попытаться материализовать groups.
			// Если bug — groups создастся здесь, потому что DependsOnInstance(shops) satisfied.
			await Task.Delay(300).ConfigureAwait(false);

			Volatile.Read(ref groupsRuns).Should().Be(0,
				"groups НЕ должен быть создан — DependsOn(employees) ещё не satisfied (employees висит на gate, нет LastSuccess)");

			Volatile.Read(ref employeesRuns).Should().BeGreaterThanOrEqualTo(1,
				"employees запустился (bootstrap keyless-стадии), но висит в ExecuteAsync");

			// Phase 2: release employees-gate.
			employeesGate.SetResult();

			// Phase 3: после employees first-success → cascade → groups[shops=u-1] создаётся.
			await AsyncWait.UntilAsync(() => Volatile.Read(ref groupsRuns) >= 1, TimeSpan.FromSeconds(5),
				"groups должен стартовать после первого успеха employees (DependsOn-канал разблокирован)");
		} finally {
			employeesGate.TrySetResult();
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task Groups_CreatedReactively_WhenEmployeesReady_AndShopsAddKey() {
		// Обратный сценарий: employees сразу завершает первый цикл (LastSuccess=now), потом shops
		// эмитит AddKey, после чего groups немедленно создаётся — даже пока shops ещё внутри
		// ExecuteAsync. Проверяет, что reactive-канал DependsOnInstance срабатывает at the moment
		// AddKey виден, а DependsOn(employees) к этому моменту satisfied.
		//
		// Topology та же, но employees НЕ gated; shops gated POSLE AddKey.

		var employeesRuns = 0;
		var shopsRuns = 0;
		var groupsRuns = 0;
		var shopsGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		var shops = new GatedStage(async (ctx, ct) => {
			if (Interlocked.Increment(ref shopsRuns) == 1) {
				await ctx.AddKeyAsync("u-1", ct);
				// Висим в ExecuteAsync ПОСЛЕ AddKey — это позволяет проверить reactive-семантику:
				// groups должен создаться, пока shops ещё running (shops.LastSuccess is null).
				using var reg = ct.Register(() => shopsGate.TrySetCanceled(ct));
				await shopsGate.Task.ConfigureAwait(false);
			}
		});

		var employees = new GatedStage((_, _) => {
			Interlocked.Increment(ref employeesRuns);
			return Task.CompletedTask;   // мгновенно завершает → LastSuccess set
		});

		var groups = new GatedStage((_, _) => {
			Interlocked.Increment(ref groupsRuns);
			return Task.CompletedTask;
		});

		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Warning);
		b.Services.AddSingleton<ShopsStub>(_ => new ShopsStub(shops));
		b.Services.AddSingleton<EmployeesStub>(_ => new EmployeesStub(employees));
		b.Services.AddSingleton<GroupsStub>(_ => new GroupsStub(groups));
		b.Services.AddInMemoryJobStateStore();
		b.Services.AddJobOrchestrator(jobs => {
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromSeconds(30));

			var shopsStage = jobs.Stage("shops")
				.HandledBy<ShopsStub>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			var employeesStage = jobs.Stage("employees")
				.HandledBy<EmployeesStub>()
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
			jobs.Stage("groups")
				.HandledBy<GroupsStub>()
				.DependsOnInstance(shopsStage)
				.DependsOn(employeesStage)
				.RunPeriodically(TimeSpan.FromMilliseconds(80));
		});

		using var host = b.Build();
		await host.StartAsync().ConfigureAwait(false);

		try {
			// employees должен пробуститься и сразу завершиться → LastSuccess set.
			await AsyncWait.UntilAsync(() => Volatile.Read(ref employeesRuns) >= 1, TimeSpan.FromSeconds(5),
				"employees должен пробуститься и завершить первый цикл");

			// shops стартует, эмитит AddKey("u-1"), висит на gate.
			await AsyncWait.UntilAsync(() => Volatile.Read(ref shopsRuns) >= 1, TimeSpan.FromSeconds(5),
				"shops должен пробуститься и эмитить AddKey");

			// Главная проверка: groups создаётся REACTIVELY — оба канала satisfied
			// (DependsOnInstance: ключ в keyspace + paired-инстанс есть;
			//  DependsOn: employees.LastSuccess != null), shops.LastSuccess не требуется.
			await AsyncWait.UntilAsync(() => Volatile.Read(ref groupsRuns) >= 1, TimeSpan.FromSeconds(5),
				"groups должен стартовать пока shops ещё в ExecuteAsync (reactive DependsOnInstance + satisfied DependsOn)");

			shopsGate.Task.IsCompleted.Should().BeFalse(
				"shops всё ещё внутри ExecuteAsync — это подтверждает, что groups создан реактивно (НЕ ждали shops.LastSuccess)");
		} finally {
			shopsGate.TrySetResult();
			await host.StopAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
		}
	}

	// Простые-тонкие adapter-stubs, чтобы каждый Stage в DI был отдельным типом для HandledBy<T>.
	private sealed class ShopsStub(GatedStage impl) : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => impl.ExecuteAsync(ctx, ct);
	}
	private sealed class EmployeesStub(GatedStage impl) : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => impl.ExecuteAsync(ctx, ct);
	}
	private sealed class GroupsStub(GatedStage impl) : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => impl.ExecuteAsync(ctx, ct);
	}

	private sealed class GatedStage(Func<JobContext, CancellationToken, Task> handler) {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => handler(ctx, ct);
	}
}
