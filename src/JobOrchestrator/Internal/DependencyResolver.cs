namespace JobOrchestrator.Internal;

/// <summary>
/// Чистые функции проверки разрешения зависимостей и поиска парных инстансов.
/// </summary>
internal static class DependencyResolver {
	/// <summary>Проверка совместимости двух словарей: общие ключи должны иметь одинаковые значения.</summary>
	public static bool AreCompatible(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b) =>
		!a.Any(kv => b.TryGetValue(kv.Key, out var bv) && !string.Equals(bv, kv.Value, StringComparison.Ordinal));

	/// <summary>
	/// Найти инстанс <paramref name="targetStageName"/>, чьи <c>DependencyKeys</c> являются проекцией
	/// <paramref name="candidateKeys"/> на множество имён ключей target-стадии (то есть все компоненты
	/// target присутствуют в candidate с теми же значениями). Для безключевой target — единственный
	/// инстанс с пустыми DependencyKeys.
	/// </summary>
	public static Instance? FindPairedInstance(InstanceManager instances, string targetStageName, IReadOnlyDictionary<string, string> candidateKeys) =>
		instances.InstancesOf(targetStageName)
			.FirstOrDefault(inst => inst.DependencyKeys.All(kv =>
				candidateKeys.TryGetValue(kv.Key, out var v) && string.Equals(v, kv.Value, StringComparison.Ordinal)));

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
		InstanceManager instances,
		KeyspaceRegistry keyspace
	) => stage.Dependencies.All(dep => {
		var paired = FindPairedInstance(instances, dep.Target.Name, keys);
		if (paired is null || paired.Metrics.LastSuccess is null) return false;
		if (dep.Mode == DependencyMode.Instance) {
			// Per-emitter check: bucket именно ЭТОГО paired-инстанса как эмитера должен содержать ключ.
			// Защищает от race-condition «keyspace.Add → KeyRemoved между построением кандидата и resolve».
			if (!keys.TryGetValue(dep.Target.Name, out var k)) return false;
			if (!keyspace.Contains(paired.Identity, k)) return false;
		}
		return true;
	});
}
