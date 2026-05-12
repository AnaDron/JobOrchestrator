using System.Collections.Concurrent;
using JobOrchestrator.Abstractions;

namespace JobOrchestrator.InMemory;

/// <summary>
/// In-memory backend для <see cref="IJobStateStore"/>. Дефолтный выбор для типового сценария «single-instance app».
/// Не персистирует данные между рестартами процесса — рестарт = полный bootstrap графа стадий с нуля.
/// </summary>
internal sealed class InMemoryJobStateStore : IJobStateStore {
	private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _scopes = new(StringComparer.Ordinal);

	public Task<string?> GetAsync(string scope, string key, CancellationToken ct) {
		if (_scopes.TryGetValue(scope, out var dict) && dict.TryGetValue(key, out var value)) {
			return Task.FromResult<string?>(value);
		}
		return Task.FromResult<string?>(null);
	}

	public Task SetAsync(string scope, string key, string value, CancellationToken ct) {
		var dict = _scopes.GetOrAdd(scope, _ => new ConcurrentDictionary<string, string>(StringComparer.Ordinal));
		dict[key] = value;
		return Task.CompletedTask;
	}

	public Task RemoveAsync(string scope, string key, CancellationToken ct) {
		if (_scopes.TryGetValue(scope, out var dict)) {
			dict.TryRemove(key, out _);
		}
		return Task.CompletedTask;
	}

	public Task RemoveScopeAsync(string scope, CancellationToken ct) {
		_scopes.TryRemove(scope, out _);
		return Task.CompletedTask;
	}
}
