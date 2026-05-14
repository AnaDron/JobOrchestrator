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
	/// Собирает финальный <see cref="StageRegistry"/>. Pipeline:
	/// <list type="number">
	/// <item><see cref="ConfigurationValidator"/>.Validate(builders): per-stage + graph structural validation.</item>
	/// <item>Inline construction bare-дескрипторов (только raw user-fields, computed = null! / -1).</item>
	/// <item><see cref="StageRegistry"/> над bare-дескрипторами (промежуточное состояние, окно не видно снаружи).</item>
	/// <item><see cref="StageInitializer"/>: pre-computes все computed-мапы один раз.</item>
	/// <item>Каждый дескриптор инициализирует себя через initializer.</item>
	/// </list>
	/// Возврат: registry с fully-initialized дескрипторами.
	/// </summary>
	internal StageRegistry BuildRegistry() {
		// Step 1: structural validation. Бросает JobConfigurationException на ошибках.
		ConfigurationValidator.Validate([.. _stages.Values]);

		// Step 2: raw-deps map для Initializer. Живёт только в этой scope.
		var rawDeps = new Dictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>>(
			_stages.Count, StringComparer.Ordinal);
		foreach (var sb in _stages.Values) {
			rawDeps[sb.Name] = sb.Dependencies;
		}

		// Step 3: bare-дескрипторы — только raw user-fields. ServiceType! безопасен,
		// ConfigurationValidator уже проверил, что задан.
		var descriptors = new List<StageDescriptor>(_stages.Count);
		foreach (var sb in _stages.Values) {
			descriptors.Add(new StageDescriptor {
				Name = sb.Name,
				ServiceType = sb.ServiceType!,
				Interval = sb.Interval,
				RetryPolicy = sb.RetryPolicy,
				Debounce = sb.Debounce,
				ExecutionTimeout = sb.ExecutionTimeout,
				ConcurrencyLimit = sb.ConcurrencyLimit,
			});
		}

		// Step 4: registry. Дескрипторы пока с computed=null! — окно видимо только внутри этого метода.
		var registry = new StageRegistry(descriptors);

		// Step 5: initializer pre-computes все мапы (граф валиден, см. Step 1).
		var initializer = new StageInitializer(registry, rawDeps);

		// Step 6: каждый дескриптор себя инициализирует. После этого registry — fully-initialized.
		foreach (var d in descriptors) d.Initialize(initializer);

		return registry;
	}
}
