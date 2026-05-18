using System.Collections.Concurrent;
using JobOrchestrator.Abstractions;

namespace JobOrchestrator.InMemory;

/// <summary>
/// In-memory backend для <see cref="IJobStateStore"/>. Дефолтный выбор для типового сценария «single-instance app».
/// Не персистирует данные между рестартами процесса — рестарт = полный bootstrap графа стадий с нуля.
/// <para>
/// Tenant-изоляция обеспечивается DI-уровнем: tenant'овый <c>AddJobOrchestrator(tenantKey, …)</c>
/// регистрирует InMemoryJobStateStore как keyed-singleton под tenantKey, каждый tenant получает свой
/// независимый экземпляр (а значит — отдельный backing <see cref="ConcurrentDictionary{TKey,TValue}"/>).
/// </para>
/// </summary>
internal sealed class InMemoryJobStateStore : IJobStateStore {
	private static readonly Task<string?> MissResult = Task.FromResult<string?>(null);

	private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _scopes = new(StringComparer.Ordinal);

	public Task<string?> GetAsync(string scope, string key, CancellationToken ct) {
		if (_scopes.TryGetValue(scope, out var dict) && dict.TryGetValue(key, out var value)) {
			return Task.FromResult<string?>(value);
		}
		return MissResult;
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
