namespace JobOrchestrator.Internal;

/// <summary>
/// Чистые функции проверки разрешения зависимостей и поиска парных инстансов.
/// </summary>
internal static class DependencyResolver {
	/// <summary>
	/// Найти инстанс <paramref name="target"/>, чьи <c>DependencyKeys</c> являются проекцией
	/// <paramref name="candidateKeys"/> на множество имён ключей target-стадии (то есть все компоненты
	/// target присутствуют в candidate с теми же значениями). Для безключевой target — единственный
	/// инстанс с пустыми DependencyKeys.
	/// </summary>
	public static Instance? FindPairedInstance(InstanceManager instances, StageDescriptor target, IReadOnlyDictionary<string, string> candidateKeys) {
		foreach (var inst in instances.InstancesOf(target)) {
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
	/// <para>
	/// Семантика (после adoption cursor's «event-driven» подхода):
	/// </para>
	/// <list type="bullet">
	/// <item>
	/// <b><c>DependsOnInstance(X)</c> — reactive:</b> требуется paired-инстанс X, <c>keys[X.Name]</c>
	/// существует и принадлежит keyspace-bucket-у этого инстанса. <c>LastSuccess</c> X-а <b>НЕ требуется</b>:
	/// сам факт публикации ключа эмитером — «событие состоялось», child-инстанс материализуется
	/// немедленно (даже пока emitter ещё внутри <c>ExecuteAsync</c>). Это поддерживает long-running
	/// emitter-pattern: shops эмитит ключи постранично, productGroups стартуют сразу для каждого.
	/// </item>
	/// <item>
	/// <b><c>DependsOn(Y)</c> — transactional:</b> требуется paired-инстанс Y с <c>LastSuccess != null</c>.
	/// Child наследует ключи только из успешного родителя (semantic fan-out по результатам полного цикла).
	/// </item>
	/// </list>
	/// </summary>
	public static bool AllDependenciesResolved(
		StageDescriptor stage,
		IReadOnlyDictionary<string, string> keys,
		InstanceManager instances,
		KeyspaceRegistry keyspace
	) {
		foreach (var dep in stage.Dependencies) {
			var paired = FindPairedInstance(instances, dep.Target, keys);
			if (paired is null) return false;
			if (dep.Mode == DependencyMode.Whole) {
				// DependsOn: child наследует ключи только успешного родителя.
				if (paired.Metrics.LastSuccess is null) return false;
			} else {
				// DependsOnInstance: reactive — LastSuccess эмитера НЕ требуется.
				// Per-emitter keyspace-check защищает от race «KeyAdded → KeyRemoved».
				if (!keys.TryGetValue(dep.Target.Name, out var k) || !keyspace.Contains(paired.Identity, k)) return false;
			}
		}
		return true;
	}
}
