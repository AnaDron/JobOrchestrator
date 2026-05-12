using JobOrchestrator.Configuration.Internal;
using JobOrchestrator.Internal;

namespace JobOrchestrator.Configuration;

/// <summary>Точка входа Fluent API для конфигурации SDK через <c>services.AddJobOrchestrator(jobs => {...})</c>.</summary>
public sealed class JobOrchestratorBuilder {
	private readonly Dictionary<string, StageBuilder> _stages = new(StringComparer.Ordinal);

	/// <summary>Дефолтные настройки, применяемые к стадиям в момент их объявления.</summary>
	public JobDefaults Defaults { get; set; } = new();

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

	internal StageRegistry BuildRegistry() {
		List<StageDescriptor> descriptors = new(_stages.Count);
		foreach (var sb in _stages.Values) {
			descriptors.Add(sb.BuildDescriptor());
		}
		return new StageRegistry(descriptors);
	}
}
