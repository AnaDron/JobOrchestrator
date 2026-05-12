namespace JobOrchestrator.Internal;

/// <summary>
/// Чистые функции проверки разрешения зависимостей и поиска парных инстансов.
/// </summary>
internal static class DependencyResolver {
	/// <summary>Проверка совместимости двух словарей: общие ключи должны иметь одинаковые значения.</summary>
	public static bool AreCompatible(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b) {
		foreach (var kv in a) {
			if (b.TryGetValue(kv.Key, out var bv) && !string.Equals(bv, kv.Value, StringComparison.Ordinal)) {
				return false;
			}
		}
		return true;
	}

	/// <summary>
	/// Найти инстанс <paramref name="targetStageName"/>, чьи <c>DependencyKeys</c> являются проекцией
	/// <paramref name="candidateKeys"/> на множество имён ключей target-стадии (то есть все компоненты
	/// target присутствуют в candidate с теми же значениями). Для безключевой target — единственный
	/// инстанс с пустыми DependencyKeys.
	/// </summary>
	public static Job? FindPairedInstance(JobManager jobs, string targetStageName, IReadOnlyDictionary<string, string> candidateKeys) {
		foreach (var inst in jobs.InstancesOf(targetStageName)) {
			bool match = true;
			foreach (var kv in inst.DependencyKeys) {
				if (!candidateKeys.TryGetValue(kv.Key, out var v) || !string.Equals(v, kv.Value, StringComparison.Ordinal)) {
					match = false;
					break;
				}
			}
			if (match) return inst;
		}
		return null;
	}

	/// <summary>
	/// Проверить, разрешены ли все зависимости стадии <paramref name="stage"/> для кандидата с ключами <paramref name="keys"/>.
	/// Семантика:
	/// <list type="bullet">
	/// <item><c>DependsOnInstance(X)</c>: <c>keys[X.Name]</c> существует, <c>keys[X.Name] ∈ keyspace(X)</c>, и парный инстанс X имеет <c>LastSuccess != null</c>.</item>
	/// <item><c>DependsOn(Y)</c>: существует инстанс Y, чьи DependencyKeys ⊆ keys и который имеет <c>LastSuccess != null</c>.</item>
	/// </list>
	/// </summary>
	public static bool AllDependenciesResolved(
		StageDescriptor stage,
		IReadOnlyDictionary<string, string> keys,
		JobManager jobs,
		KeyspaceRegistry keyspace
	) {
		foreach (var dep in stage.Dependencies) {
			if (dep.Mode == DependencyMode.Instance) {
				if (!keys.TryGetValue(dep.TargetStageName, out var k)) return false;
				if (!keyspace.Contains(dep.TargetStageName, k)) return false;
				var paired = FindPairedInstance(jobs, dep.TargetStageName, keys);
				if (paired is null || paired.LastSuccess is null) return false;
			} else {
				var paired = FindPairedInstance(jobs, dep.TargetStageName, keys);
				if (paired is null || paired.LastSuccess is null) return false;
			}
		}
		return true;
	}
}
