using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Race: cascade помечает Running-инстанс Terminating и cancel'ит runner; runner всё равно публикует
/// StageCompleted. <see cref="EventLoop.FinalizeTerminatingAsync"/> должен отработать ровно один раз
/// (в т.ч. не вызывать <see cref="IJobStateStore.RemoveScopeAsync"/> повторно).
/// </summary>
public sealed class ScenarioCascadeFinalizeRaceTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

	[Fact]
	public async Task CascadeCancelRunningInstance_FinalizesOnce_RemoveScopeCalledOnce() {
		var iterationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var releaseIteration = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		var shopsFake = new FakeServiceA();
		shopsFake.ExecuteHandler = async (ctx, ct) => { await ctx.AddKeyAsync("u1", ct); };

		var pgFake = new FakeServiceB();
		pgFake.ExecuteHandler = async (_, ct) => {
			iterationStarted.TrySetResult();
			await releaseIteration.Task.WaitAsync(ct).ConfigureAwait(false);
		};

		var scopeRemoveCounts = new Dictionary<string, int>(StringComparer.Ordinal);
		using var host = TestHostFactory.Build(
			configure: jobs => {
				var shops = jobs.Stage("shops").HandledBy<FakeServiceA>().RunPeriodically(TimeSpan.FromHours(1));
				jobs.Stage("pg")
					.HandledBy<FakeServiceB>()
					.DependsOnInstance(shops)
					.RunPeriodically(TimeSpan.FromHours(1));
			},
			registerFakes: s => {
				s.AddSingleton<FakeServiceA>(shopsFake);
				s.AddSingleton<FakeServiceB>(pgFake);
				s.AddSingleton<IJobStateStore>(new CountingJobStateStore(scopeRemoveCounts));
			});

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
			await iterationStarted.Task.WaitAsync(Timeout).ConfigureAwait(false);

			var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();
			orchestrator.Root["shops"].UnregisterKey("u1");

			releaseIteration.TrySetResult();

			const string scope = "pg:shops=u1";
			(await TestSync.WaitForAsync(
				() => scopeRemoveCounts.TryGetValue(scope, out int c) && c >= 1,
				Timeout).ConfigureAwait(false)).Should().BeTrue();
			scopeRemoveCounts[scope].Should().Be(1, "двойной FinalizeTerminating не должен вызывать RemoveScopeAsync повторно");
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}
	}

	private sealed class CountingJobStateStore(Dictionary<string, int> scopeRemoveCounts) : IJobStateStore {
		private readonly Dictionary<string, Dictionary<string, string>> _scopes = new(StringComparer.Ordinal);

		public Task<string?> GetAsync(string scope, string key, CancellationToken ct) {
			lock (_scopes) {
				return Task.FromResult(_scopes.TryGetValue(scope, out var keys) && keys.TryGetValue(key, out var v) ? v : null);
			}
		}

		public Task SetAsync(string scope, string key, string value, CancellationToken ct) {
			lock (_scopes) {
				if (!_scopes.TryGetValue(scope, out var keys)) {
					keys = new Dictionary<string, string>(StringComparer.Ordinal);
					_scopes[scope] = keys;
				}
				keys[key] = value;
			}
			return Task.CompletedTask;
		}

		public Task RemoveAsync(string scope, string key, CancellationToken ct) {
			lock (_scopes) {
				if (_scopes.TryGetValue(scope, out var keys)) keys.Remove(key);
			}
			return Task.CompletedTask;
		}

		public Task RemoveScopeAsync(string scope, CancellationToken ct) {
			lock (_scopes) {
				_scopes.Remove(scope);
				scopeRemoveCounts.TryGetValue(scope, out int count);
				scopeRemoveCounts[scope] = count + 1;
			}
			return Task.CompletedTask;
		}
	}
}
