using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Hosting.Keyed;

/// <summary>
/// Wrapper-провайдер для авто-пропагации tenant-ключа при сборке keyed-сервиса через
/// <see cref="ActivatorUtilities.CreateInstance"/>: каждый <see cref="GetService"/>-запрос
/// сначала пробует keyed-вариант с зафиксированным ключом, при отсутствии — fallback на
/// root non-keyed. Снимает необходимость вручную дёргать <c>GetRequiredKeyedService</c>
/// для каждой зависимости в registration-factory.
/// <para>
/// <b>Scope-propagation:</b> запрос <see cref="IServiceScopeFactory"/> возвращает
/// <see cref="KeyedAwareScopeFactory"/>, чтобы scope, созданный из этого провайдера, тоже
/// был keyed-aware. Цепочка: provider → scope.ServiceProvider → ... — везде сохраняется тот же
/// привязанный ключ.
/// </para>
/// <para>
/// <b>Жизненный цикл:</b> создаётся on-the-fly внутри factory у keyed-singleton'а; живёт
/// столько же, сколько сам зарезолвленный сервис (singleton — на весь host'а; scoped —
/// на scope). Не регистрируется в DI — это утилитарный wrapper.
/// </para>
/// </summary>
internal sealed class KeyedAwareServiceProvider(IServiceProvider inner, object key)
	: IServiceProvider, IKeyedServiceProvider, ISupportRequiredService {

	public object? GetService(Type serviceType) {
		// Если запросили IServiceProvider — отдаём себя, чтобы descendants (StageRunner и т.п.)
		// тоже резолвили зависимости keyed-aware.
		if (serviceType == typeof(IServiceProvider)) return this;
		// Перехват scope-factory: scope, созданный из этого провайдера, тоже должен быть keyed-aware.
		if (serviceType == typeof(IServiceScopeFactory)) {
			return new KeyedAwareScopeFactory(
				(IServiceScopeFactory)inner.GetService(typeof(IServiceScopeFactory))!,
				key);
		}
		var keyed = ((IKeyedServiceProvider)inner).GetKeyedService(serviceType, key);
		return keyed ?? inner.GetService(serviceType);
	}

	public object? GetKeyedService(Type serviceType, object? serviceKey) =>
		((IKeyedServiceProvider)inner).GetKeyedService(serviceType, serviceKey);

	public object GetRequiredKeyedService(Type serviceType, object? serviceKey) =>
		((IKeyedServiceProvider)inner).GetRequiredKeyedService(serviceType, serviceKey);

	public object GetRequiredService(Type serviceType) =>
		GetService(serviceType) ?? throw new InvalidOperationException(
			$"Не удалось разрезолвить сервис {serviceType} ни как keyed ('{key}'), ни как non-keyed.");
}
