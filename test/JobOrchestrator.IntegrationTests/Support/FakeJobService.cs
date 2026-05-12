namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Тестовый <see cref="IJobService"/>: записывает все полученные <see cref="JobContext"/>,
/// сигналит TCS при каждом запуске, позволяет внедрять custom-логику ExecuteAsync.
/// Регистрируется в DI как singleton (общий экземпляр через все scope'ы итераций).
/// </summary>
internal abstract class FakeJobServiceBase : IJobService {
	private readonly List<JobContext> _calls = [];
	private readonly object _gate = new();
	private TaskCompletionSource _anyCallTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

	/// <summary>Опциональный delegate для управления телом ExecuteAsync (по умолчанию — мгновенный успех).</summary>
	public Func<JobContext, CancellationToken, Task>? ExecuteHandler { get; set; }

	public IReadOnlyList<JobContext> Calls {
		get { lock (_gate) return [.. _calls]; }
	}

	public int CallCount {
		get { lock (_gate) return _calls.Count; }
	}

	public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		lock (_gate) {
			_calls.Add(ctx);
		}
		var localTcs = _anyCallTcs;
		localTcs.TrySetResult();

		if (ExecuteHandler is { } handler) {
			await handler(ctx, ct).ConfigureAwait(false);
		}
	}

	/// <summary>Ожидание следующего любого вызова ExecuteAsync (с таймаутом).</summary>
	public async Task<bool> WaitForNextCallAsync(TimeSpan timeout) {
		Task task;
		lock (_gate) {
			task = _anyCallTcs.Task;
		}
		var completed = await Task.WhenAny(task, Task.Delay(timeout)).ConfigureAwait(false);
		if (completed != task) return false;
		lock (_gate) {
			_anyCallTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		}
		return true;
	}

	/// <summary>Ожидание пока CallCount достигнет <paramref name="target"/> (с таймаутом).</summary>
	public async Task<bool> WaitForCallCountAsync(int target, TimeSpan timeout) {
		var sw = System.Diagnostics.Stopwatch.StartNew();
		while (sw.Elapsed < timeout) {
			if (CallCount >= target) return true;
			await Task.Delay(20).ConfigureAwait(false);
		}
		return CallCount >= target;
	}
}

internal sealed class FakeServiceA : FakeJobServiceBase { }
internal sealed class FakeServiceB : FakeJobServiceBase { }
internal sealed class FakeServiceC : FakeJobServiceBase { }
internal sealed class FakeServiceD : FakeJobServiceBase { }
internal sealed class FakeServiceE : FakeJobServiceBase { }
