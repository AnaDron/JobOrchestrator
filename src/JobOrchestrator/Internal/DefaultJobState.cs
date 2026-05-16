using System.Text.Json;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobState"/> поверх <see cref="IJobStateStore"/> с JSON-сериализацией значений.
/// Scope формируется как <c>"{StageName}:{EncodedDependencyKeys}"</c> — уникален per-инстанс.
/// </summary>
internal sealed class DefaultJobState(IJobStateStore store, string scope) : IJobState {
	private static readonly JsonSerializerOptions JsonOptions = new() {
		PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
	};

	public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default) {
		var raw = await store.GetAsync(scope, key, ct).ConfigureAwait(false);
		if (raw is null) return default;
		return JsonSerializer.Deserialize<T>(raw, JsonOptions);
	}

	public Task SetAsync<T>(string key, T value, CancellationToken ct = default) {
		var raw = JsonSerializer.Serialize(value, JsonOptions);
		return store.SetAsync(scope, key, raw, ct);
	}

	public Task RemoveAsync(string key, CancellationToken ct = default) {
		return store.RemoveAsync(scope, key, ct);
	}
}
