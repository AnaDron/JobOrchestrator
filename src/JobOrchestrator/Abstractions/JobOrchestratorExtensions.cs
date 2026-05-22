namespace JobOrchestrator.Abstractions;

/// <summary>Удобные расширения над <see cref="IJobOrchestrator"/> — диагностический срез и full-name lookup.</summary>
public static class JobOrchestratorExtensions {
	/// <summary>
	/// Все зарегистрированные стадии оркестратора без разбора домена. SelectMany по доменам —
	/// порядок: сначала стадии root-домена, затем по доменам в порядке регистрации.
	/// </summary>
	public static IReadOnlyCollection<IStageHandle> AllStages(this IJobOrchestrator self) {
		ArgumentNullException.ThrowIfNull(self);
		var result = new List<IStageHandle>();
		foreach (var domain in self) {
			foreach (var stage in domain) result.Add(stage);
		}
		return result;
	}

	/// <summary>
	/// Lookup стадии по полному имени (<c>"domain:local"</c> или просто <c>"local"</c> для root-домена).
	/// Удобство для full-name из конфигурации/логов — без ручного парсинга domain-separator'а.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадии с таким полным именем нет.</exception>
	public static IStageHandle GetStage(this IJobOrchestrator self, string fullName) {
		ArgumentNullException.ThrowIfNull(self);
		ArgumentException.ThrowIfNullOrEmpty(fullName);
		foreach (var domain in self) {
			foreach (var stage in domain) {
				if (string.Equals(stage.Name, fullName, StringComparison.Ordinal)) return stage;
			}
		}
		throw new ArgumentException($"Стадия с полным именем '{fullName}' не зарегистрирована.", nameof(fullName));
	}
}
