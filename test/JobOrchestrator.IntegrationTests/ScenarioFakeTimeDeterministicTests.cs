using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Time.Testing;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Детерминированные тесты на основе <see cref="FakeTimeProvider"/>: проверяем что планирование
/// Auto-тиков (interval) и retry-delay подчиняются виртуальному времени, а не реальному wall-clock.
/// <para>
/// Архитектура SDK: <c>DueScanner.RunAsync</c> делает <c>Task.Delay(sleep, time, ct)</c> через
/// инжектированный <see cref="TimeProvider"/>. Регистрация <see cref="FakeTimeProvider"/> ДО
/// <c>AddJobOrchestrator</c> в <see cref="TestHostFactory.Build"/> перехватывает дефолтный
/// <c>TryAddSingleton(TimeProvider.System)</c>, поэтому сканер «спит» пока тест не вызовет
/// <see cref="FakeTimeProvider.Advance"/> — устраняет sleep-based ожидания вида
/// <c>await Task.Delay(...)</c> внутри теста.
/// </para>
/// </summary>
public sealed class ScenarioFakeTimeDeterministicTests {
	// Лимит на ожидание propagation события через Channel — это про реальное время, не про виртуальное.
	private static readonly TimeSpan EventPropagationTimeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task RetryDelay_DoesNotElapseUntilFakeTimeAdvances() {
		var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
		var fake = new FakeServiceA();
		int attempts = 0;
		fake.ExecuteHandler = (_, _) => {
			int n = Interlocked.Increment(ref attempts);
			if (n <= 2) throw new InvalidOperationException($"fail {n}");
			return Task.CompletedTask;
		};

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("x")
				.HandledBy<FakeServiceA>()
				.RunPeriodically(TimeSpan.FromHours(1))
				.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromMinutes(5))),
			registerFakes: s => s.AddSingleton(fake),
			timeProvider: time);

		await host.StartAsync().ConfigureAwait(false);
		try {
			// 1-й вызов сразу при bootstrap (NextAutoUtc = now → DueScanner подбирает в первой итерации
			// после Wake() из BootstrapInitialInstances). Падает с InvalidOperationException.
			(await fake.WaitForCallCountAsync(1, EventPropagationTimeout).ConfigureAwait(false))
				.Should().BeTrue("первый вызов должен случиться без advance — Bootstrap взвёл NextAutoUtc=now");

			// Без advance — никакого retry. Если бы время шло реально, через 5 минут был бы 2-й вызов;
			// при FakeTime — нет, пока мы явно не сдвинули.
			await Task.Delay(150).ConfigureAwait(false);   // даём event loop'у переварить StageFailed
			fake.CallCount.Should().Be(1, "FakeTime не advance → retry-delay не истёк");

			// Advance меньше retry-delay → всё ещё нет retry.
			time.Advance(TimeSpan.FromMinutes(4));
			await Task.Delay(150).ConfigureAwait(false);
			fake.CallCount.Should().Be(1, "4 мин < 5 мин retry-delay → DueScanner ещё спит");

			// Advance до полной retry-delay → DueScanner просыпается, фиксирует due, публикует TimerTicked.
			time.Advance(TimeSpan.FromMinutes(1));
			(await fake.WaitForCallCountAsync(2, EventPropagationTimeout).ConfigureAwait(false))
				.Should().BeTrue("после 5 мин retry-delay DueScanner должен опубликовать TimerTicked");

			// 2-й вызов снова fail. Advance ещё 5 минут → 3-й вызов (success).
			time.Advance(TimeSpan.FromMinutes(5));
			(await fake.WaitForCallCountAsync(3, EventPropagationTimeout).ConfigureAwait(false))
				.Should().BeTrue("3-й вызов после второго retry-delay");

			// 3-й = success → NextAutoUtc = now + 1 час. Без advance не должно быть 4-го вызова.
			await Task.Delay(200).ConfigureAwait(false);
			fake.CallCount.Should().Be(3, "после success NextAutoUtc = now + 1ч; advance мы не делали");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	[Fact]
	public async Task PeriodicInterval_NextTickFiresAfterIntervalAdvance() {
		var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
		var fake = new FakeServiceA();
		var interval = TimeSpan.FromMinutes(10);

		using var host = TestHostFactory.Build(
			configure: jobs => jobs.Stage("x").HandledBy<FakeServiceA>().RunPeriodically(interval),
			registerFakes: s => s.AddSingleton(fake),
			timeProvider: time);

		await host.StartAsync().ConfigureAwait(false);
		try {
			// Bootstrap-тик: NextAutoUtc = now → первый вызов сразу.
			(await fake.WaitForCallCountAsync(1, EventPropagationTimeout).ConfigureAwait(false)).Should().BeTrue();

			// Без advance interval=10мин повторного тика быть не должно.
			await Task.Delay(150).ConfigureAwait(false);
			fake.CallCount.Should().Be(1);

			// Advance ровно interval → DueScanner просыпается, второй тик.
			time.Advance(interval);
			(await fake.WaitForCallCountAsync(2, EventPropagationTimeout).ConfigureAwait(false)).Should().BeTrue();

			// Ещё advance interval → третий тик.
			time.Advance(interval);
			(await fake.WaitForCallCountAsync(3, EventPropagationTimeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}
}
