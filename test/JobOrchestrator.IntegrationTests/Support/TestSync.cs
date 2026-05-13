namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Helper для signal-based-ожидания вместо хрупкого <c>await Task.Delay(...)</c>:
/// poll-цикл с коротким интервалом до выполнения предиката или истечения таймаута.
/// </summary>
internal static class TestSync {
	/// <summary>Опрашивает <paramref name="predicate"/> каждые 25 мс до <c>true</c> или таймаута.</summary>
	public static async Task<bool> WaitForAsync(Func<bool> predicate, TimeSpan timeout) {
		var sw = System.Diagnostics.Stopwatch.StartNew();
		while (sw.Elapsed < timeout) {
			if (predicate()) return true;
			await Task.Delay(25).ConfigureAwait(false);
		}
		return predicate();
	}

	/// <summary>То же, но предикат асинхронный (например, читающий внешний счётчик через await).</summary>
	public static async Task<bool> WaitForAsync(Func<Task<bool>> predicate, TimeSpan timeout) {
		var sw = System.Diagnostics.Stopwatch.StartNew();
		while (sw.Elapsed < timeout) {
			if (await predicate().ConfigureAwait(false)) return true;
			await Task.Delay(25).ConfigureAwait(false);
		}
		return await predicate().ConfigureAwait(false);
	}
}
