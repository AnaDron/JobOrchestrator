namespace JobOrchestrator.Internal;

/// <summary>
/// Тонкий by-name индекс <see cref="StageDescriptor"/>-ов.
/// <para>
/// После Phase A/B/C рефакторинга вся «графовая» логика — транзитивный
/// <see cref="StageDescriptor.ExpectedKeyNames"/>, reverse-индексы
/// (<see cref="StageDescriptor.DependentsWhole"/>/<see cref="StageDescriptor.DependentsInstance"/>),
/// транзитивное замыкание <see cref="StageDescriptor.AffectedByKeyRemoval"/>, и
/// <see cref="StageDescriptor.CancellationRank"/> — живут на самих дескрипторах. DI-валидация
/// (<c>ValidateServiceRegistrations</c>) переехала в <see cref="Configuration.Internal.ConfigurationValidator"/>.
/// Registry теперь — чистый словарь имя→дескриптор, ничего более.
/// </para>
/// </summary>
internal sealed class StageRegistry {
	private readonly Dictionary<string, StageDescriptor> _byName;

	public StageRegistry(IReadOnlyList<StageDescriptor> stages) {
		ArgumentNullException.ThrowIfNull(stages);
		_byName = new Dictionary<string, StageDescriptor>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) {
			if (!_byName.TryAdd(s.Name, s)) {
				throw new JobConfigurationException($"Дубль имени стадии: '{s.Name}'.");
			}
		}
	}

	public StageDescriptor Get(string name) =>
		_byName.TryGetValue(name, out var s)
			? s
			: throw new KeyNotFoundException($"Стадия '{name}' не найдена в реестре.");

	public IReadOnlyCollection<StageDescriptor> AllStages => _byName.Values;
}
