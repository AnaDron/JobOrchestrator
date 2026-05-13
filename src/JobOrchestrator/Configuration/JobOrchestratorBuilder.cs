using JobOrchestrator.Configuration.Internal;
using JobOrchestrator.Internal;

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

	internal StageRegistry BuildRegistry() =>
		new StageRegistry(_stages.Values.Select(sb => sb.BuildDescriptor()).ToList());
}
