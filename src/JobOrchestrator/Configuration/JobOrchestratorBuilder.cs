using JobOrchestrator.Configuration.Internal;

namespace JobOrchestrator.Configuration;

/// <summary>Точка входа Fluent API для конфигурации SDK через <c>services.AddJobOrchestrator(jobs => {...})</c>.</summary>
public sealed class JobOrchestratorBuilder {
	/// <summary>Разделитель domain-префикса и имени стадии в полном FQN-имени (<c>"evotor:shops"</c>).</summary>
	internal const char DomainSeparator = ':';

	private readonly Dictionary<string, StageBuilder> _stages = new(StringComparer.Ordinal);
	private JobDefaults _defaults = new();
	private string? _currentDomain;

	/// <summary>
	/// Дефолтные настройки, применяемые к стадиям в момент их объявления.
	/// Присваивание <c>null</c> пересоздаёт пустой <see cref="JobDefaults"/> — guard от случайного NRE в <c>StageBuilder</c>.
	/// </summary>
	public JobDefaults Defaults {
		get => _defaults;
		set => _defaults = value ?? new JobDefaults();
	}

	/// <summary>
	/// Вход в domain-блок (state-based). Все последующие <see cref="Stage"/>-вызовы до
	/// <see cref="WithoutDomain"/> получают префикс <c>"{domain}:"</c> в имени.
	/// <para>
	/// Вложенные domain'ы запрещены: повторный вызов без сброса бросает <see cref="JobConfigurationException"/>.
	/// Action-scoped вариант (<see cref="WithDomain(string, Action{JobOrchestratorBuilder})"/>) предпочтительнее —
	/// он гарантирует автоматический сброс state-а через <c>try/finally</c>.
	/// </para>
	/// </summary>
	public JobOrchestratorBuilder WithDomain(string domain) {
		EnterDomain(domain);
		return this;
	}

	/// <summary>
	/// Запускает <paramref name="configure"/> внутри domain-блока. Domain активен ТОЛЬКО на
	/// время выполнения action; восстанавливается через <c>try/finally</c> после выхода, даже при
	/// исключении внутри. Защищает от случая «забыли <see cref="WithoutDomain"/>».
	/// <para>
	/// Вложенные <c>WithDomain</c> любого стиля (state-based или action-scoped) — бросают
	/// <see cref="JobConfigurationException"/>.
	/// </para>
	/// </summary>
	public JobOrchestratorBuilder WithDomain(string domain, Action<JobOrchestratorBuilder> configure) {
		ArgumentNullException.ThrowIfNull(configure);
		var saved = _currentDomain;
		EnterDomain(domain);
		try {
			configure(this);
		} finally {
			_currentDomain = saved;
		}
		return this;
	}

	/// <summary>
	/// Сбрасывает domain-state, установленный через <see cref="WithDomain(string)"/>.
	/// После — <see cref="Stage"/> возвращает имя без префикса. Идемпотентно: повторный вызов на
	/// уже-сброшенном state — no-op без exception.
	/// </summary>
	public JobOrchestratorBuilder WithoutDomain() {
		_currentDomain = null;
		return this;
	}

	/// <summary>Объявить новую стадию. Снимок текущих <see cref="Defaults"/> захватывается в этот момент.
	/// <para>
	/// Если активен domain-блок (<see cref="WithDomain(string)"/> или <see cref="WithDomain(string, Action{JobOrchestratorBuilder})"/>),
	/// реальное имя стадии — <c>"{domain}:{name}"</c>. Имя <paramref name="name"/> не должно содержать <c>':'</c>
	/// (разделитель зарезервирован для domain-префикса).
	/// </para>
	/// </summary>
	public IStageBuilder Stage(string name) {
		ArgumentException.ThrowIfNullOrEmpty(name);
		if (name.Contains(DomainSeparator)) {
			throw new JobConfigurationException(
				$"Имя стадии '{name}' содержит зарезервированный символ '{DomainSeparator}' — используйте WithDomain для префикса.");
		}
		var fullName = _currentDomain is null ? name : $"{_currentDomain}{DomainSeparator}{name}";
		if (_stages.ContainsKey(fullName)) {
			throw new JobConfigurationException($"Стадия с именем '{fullName}' уже объявлена.");
		}
		var sb = new StageBuilder(fullName, Defaults);
		_stages.Add(fullName, sb);
		return sb;
	}

	/// <summary>
	/// Валидация и установка <c>_currentDomain</c>. Общая для обоих стилей <c>WithDomain</c>.
	/// </summary>
	private void EnterDomain(string domain) {
		ArgumentException.ThrowIfNullOrEmpty(domain);
		if (domain.Contains(DomainSeparator)) {
			throw new JobConfigurationException(
				$"Domain '{domain}' содержит зарезервированный символ '{DomainSeparator}' — вложенные domain'ы не поддерживаются.");
		}
		if (_currentDomain is not null) {
			throw new JobConfigurationException(
				$"Domain уже установлен в '{_currentDomain}'. Вложенные WithDomain не поддерживаются — используйте WithoutDomain() перед сменой.");
		}
		_currentDomain = domain;
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
