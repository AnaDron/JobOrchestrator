namespace JobOrchestrator.IntegrationTests.Support;

// Stage-services для типового Evotor-сценария + cartesian-теста.
// Каждый сервис записывает свой execution в ExecutionRecorder и (опционально) эмитит ключи.

internal sealed class ShopsStageService(ExecutionRecorder recorder, ShopsKeySource keys) : IJobService {
	public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		foreach (var k in keys.SnapshotAddNext()) await ctx.AddKeyAsync(k, ct);
		foreach (var k in keys.SnapshotRemoveNext()) await ctx.RemoveKeyAsync(k, ct);
	}
}

internal sealed class ProductGroupsStageService(ExecutionRecorder recorder) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		return Task.CompletedTask;
	}
}

internal sealed class ProductsStageService(ExecutionRecorder recorder) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		return Task.CompletedTask;
	}
}

internal sealed class EmployeesStageService(ExecutionRecorder recorder) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		return Task.CompletedTask;
	}
}

internal sealed class DocumentsStageService(ExecutionRecorder recorder) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		return Task.CompletedTask;
	}
}

internal sealed class CurrenciesStageService(ExecutionRecorder recorder, CurrenciesKeySource keys) : IJobService {
	public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		recorder.Record(ctx);
		foreach (var k in keys.SnapshotAddNext()) await ctx.AddKeyAsync(k, ct);
	}
}

/// <summary>
/// Очередь batch-ей ключей для последовательных вызовов <c>ExecuteAsync</c>. Каждый вызов забирает
/// один batch — это позволяет тестам шедулить разные паттерны эмиссии (например, первый цикл эмитит,
/// второй — нет; третий — RemoveKey).
/// </summary>
internal sealed class ShopsKeySource {
	private readonly Queue<string[]> _addBatches = new();
	private readonly Queue<string[]> _removeBatches = new();

	public void EnqueueAdds(params string[] keys) => _addBatches.Enqueue(keys);
	public void EnqueueRemoves(params string[] keys) => _removeBatches.Enqueue(keys);
	public IEnumerable<string> SnapshotAddNext() => _addBatches.TryDequeue(out var b) ? b : [];
	public IEnumerable<string> SnapshotRemoveNext() => _removeBatches.TryDequeue(out var b) ? b : [];
}

internal sealed class CurrenciesKeySource {
	private readonly Queue<string[]> _batches = new();
	public void Enqueue(params string[] keys) => _batches.Enqueue(keys);
	public IEnumerable<string> SnapshotAddNext() => _batches.TryDequeue(out var b) ? b : [];
}
