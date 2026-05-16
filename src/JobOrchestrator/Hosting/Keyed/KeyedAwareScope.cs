using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Hosting.Keyed;

/// <summary>
/// Scope, чей <see cref="ServiceProvider"/> является <see cref="KeyedAwareServiceProvider"/>:
/// auto-резолв keyed-сервисов с зафиксированным ключом продолжается и внутри scope.
/// Создаётся через <see cref="KeyedAwareScopeFactory"/>.
/// <para>
/// Реализует <see cref="IAsyncDisposable"/> через делегирование: если <paramref name="inner"/>
/// поддерживает async-dispose (как DefaultServiceProvider в .NET 8+), используем его — это
/// критично для scoped-сервисов с асинхронными ресурсами (например, EF DbContext с открытыми
/// транзакциями).
/// </para>
/// </summary>
internal sealed class KeyedAwareScope(IServiceScope inner, object key) : IServiceScope, IAsyncDisposable {
	public IServiceProvider ServiceProvider { get; } = new KeyedAwareServiceProvider(inner.ServiceProvider, key);

	public void Dispose() => inner.Dispose();

	public ValueTask DisposeAsync() {
		if (inner is IAsyncDisposable async) return async.DisposeAsync();
		inner.Dispose();
		return default;
	}
}
