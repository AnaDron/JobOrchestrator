using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Hosting.Keyed;

/// <summary>
/// Scope factory, продуцирующая <see cref="KeyedAwareScope"/>-ы вместо стандартных scope'ов.
/// Создаётся внутри <see cref="KeyedAwareServiceProvider"/> при запросе
/// <see cref="IServiceScopeFactory"/> — это и есть точка, где keyed-aware-семантика
/// «пробрасывается» в scope-уровень DI-графа.
/// </summary>
internal sealed class KeyedAwareScopeFactory(IServiceScopeFactory inner, object key) : IServiceScopeFactory {
	public IServiceScope CreateScope() => new KeyedAwareScope(inner.CreateScope(), key);
}
