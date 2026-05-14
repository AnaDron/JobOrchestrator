using JobOrchestrator.Configuration.Internal;

namespace JobOrchestrator.Configuration;

/// <summary>Точка входа Fluent API для конфигурации SDK через <c>services.AddJobOrchestrator(jobs => {...})</c>.</summary>
public sealed class JobOrchestratorBuilder {
	private readonly Dictionary<string, StageBuilder> _stages = new(StringComparer.Ordinal);
	private JobDefaults _defaults = new();

	/// <summary>
	/// Дефолтные настройки, применяемые к стадиям в момент их объявления.
	/// Присваивание <c>null</c> пересоздаёт пустой <see cref="JobDefaults"/> — guard от случайного NRE в <c>StageBuilder</c>.
	/// </summary>
	public JobDefaults Defaults {
		get => _defaults;
		set => _defaults = value ?? new JobDefaults();
	}

	/// <summary>Объявить новую стадию. Снимок текущих <see cref="Defaults"/> захватывается в этот момент.</summary>
	public IStageBuilder Stage(string name) {
		ArgumentException.ThrowIfNullOrEmpty(name);
		if (_stages.ContainsKey(name)) {
			throw new JobConfigurationException($"Стадия с именем '{name}' уже объявлена.");
		}
		var sb = new StageBuilder(name, Defaults);
		_stages.Add(name, sb);
		return sb;
	}

	/// <summary>
	/// Собирает финальный <see cref="StageRegistry"/> с fully-initialized дескрипторами. Pipeline:
	/// <list type="number">
	/// <item>raw-deps map by-name + <see cref="ConfigurationValidator.Validate"/> (per-stage конфиг
	/// и structural-checks: висячие зависимости, циклы). Map переиспользуется в Step 4.</item>
	/// <item>Inline construction bare-дескрипторов: только raw user-fields, computed-поля
	/// остаются в <c>null!</c> / <c>-1</c>-defaults до Step 5.</item>
	/// <item><see cref="StageRegistry"/> над bare-дескрипторами. Промежуточное состояние —
	/// computed ещё null!; окно не видно снаружи, регистр возвращается только после Step 5.</item>
	/// <item><see cref="StageInitializer"/>: pre-compute by-name мап один раз (deps refs, transitive
	/// ExpectedKeyNames, reverse-индексы, cancellation rank). Reuse rawDeps из Step 1.</item>
	/// <item>Каждый дескриптор инициализирует себя через <see cref="StageDescriptor.Initialize"/>
	/// — type-token authorization: вызвать можно только имея <see cref="IStageInitializer"/>.</item>
	/// </list>
	/// </summary>
	internal StageRegistry BuildRegistry() {
		// Step 1: raw-deps map + per-stage и structural валидация. Map переиспользуется в Step 4.
		// Валидатор предполагает граф проверяемым уже на этом шаге — последующие шаги опираются
		// на отсутствие циклов (recursive compute в StageInitializer).
		var rawDeps = _stages.Values.ToDictionary(x => x.Name, x => x.Dependencies, StringComparer.Ordinal);
		ConfigurationValidator.Validate(_stages.Values, rawDeps);

		// Step 2: bare-дескрипторы со взятыми у builder-ов raw-полями. Computed-поля
		// (Dependencies, ExpectedKeyNames, Dependents*, AffectedByKeyRemoval, CancellationRank)
		// остаются default'ными (null! / -1) до Step 5. ServiceType! безопасен — Step 1 проверил.
		var descriptors = _stages.Values.Select(sb => new StageDescriptor {
			Name = sb.Name,
			ServiceType = sb.ServiceType!,
			Interval = sb.Interval,
			RetryPolicy = sb.RetryPolicy,
			Debounce = sb.Debounce,
			ExecutionTimeout = sb.ExecutionTimeout,
			ConcurrencyLimit = sb.ConcurrencyLimit,
		}).ToList();

		// Step 3: registry. Дескрипторы внутри ещё с null!-computed-полями — допустимо только
		// потому, что наружу регистр уйдёт лишь после Step 5 (атомарность с точки зрения callers).
		var registry = new StageRegistry(descriptors);

		// Step 4: pre-compute by-name мап. Initializer резолвит string-имена в descriptor-refs
		// через registry, поэтому требует уже-построенный registry из Step 3.
		var initializer = new StageInitializer(registry, rawDeps);

		// Step 5: type-token authorized init. После цикла все computed-поля каждого дескриптора
		// заполнены, registry содержит fully-initialized дескрипторы.
		foreach (var d in descriptors) d.Initialize(initializer);

		return registry;
	}
}
