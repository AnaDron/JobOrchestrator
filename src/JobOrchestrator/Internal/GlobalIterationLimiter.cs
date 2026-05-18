namespace JobOrchestrator.Internal;

/// <summary>
/// Глобальный лимит одновременных итераций по всем стадиям (поверх per-stage <see cref="ConcurrencyLimits"/>).
/// </summary>
internal sealed class GlobalIterationLimiter : IDisposable {
	private readonly SemaphoreSlim? _sem;

	public GlobalIterationLimiter(StageRegistry registry) {
		if (registry.GlobalConcurrencyLimit is { } limit) {
			if (limit < 1) {
				throw new ArgumentOutOfRangeException(nameof(registry), limit,
					"GlobalConcurrencyLimit должен быть >= 1.");
			}
			_sem = new SemaphoreSlim(limit, limit);
		}
	}

	public bool TryAcquire() => _sem is null || _sem.Wait(0);

	public void Release() {
		if (_sem is not null) _sem.Release();
	}

	public void Dispose() {
		_sem?.Dispose();
		GC.SuppressFinalize(this);
	}
}
