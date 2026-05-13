namespace JobOrchestrator.Internal;

/// <summary>
/// Создаёт инстансы стадий по мере разрешения зависимостей.
/// <para>
/// Алгоритм — backtracking с partial-merge:
/// </para>
/// <list type="number">
/// <item>Для каждой зависимости стадии формируется «измерение» candidate-ключей.</item>
/// <item>Инкрементальный обход измерений: на каждом шаге пробуем добавить candidate к накопленному merged-словарю.</item>
/// <item>Если candidate несовместим с уже накопленным — отбрасываем ВСЮ ветку без перебора оставшихся измерений (early pruning).</item>
/// <item>Когда все измерения пройдены — проверяем разрешение зависимостей и создаём инстанс.</item>
/// </list>
/// <para>
/// Это даёт типичную сложность O(произведение_совместимых_путей) вместо O(полного_cartesian),
/// что критично для графов с общим ключом через все зависимости (например, shops=u_i проходит сквозь весь Эвотор-граф).
/// </para>
/// <para>
/// Insertion order компонентов в результирующем словаре нормализуется по <c>stage.Dependencies</c>
/// (= порядок объявления в Fluent API), что обеспечивает стабильный FullyQualifiedName независимо
/// от того, в каком порядке прилетали разрешающие события.
/// </para>
/// </summary>
internal sealed class InstanceCreator(InstanceManager instances, KeyspaceRegistry keyspace, TimeProvider time) {
	public List<StageInstance> EvaluateAndCreate(StageDescriptor stage) {
		if (stage.Dependencies.Count == 0) {
			// Безключевая стадия → один инстанс с пустыми DependencyKeys.
			var empty = new Dictionary<string, string>(StringComparer.Ordinal);
			return instances.Exists(stage.Name, empty) ? [] : [MaterializeInstance(stage, empty)];
		}

		// Собираем измерения candidate-ключей.
		var dimensions = new List<List<IReadOnlyDictionary<string, string>>>(stage.Dependencies.Count);
		foreach (var dep in stage.Dependencies) {
			var dim = ComputeDimension(dep);
			if (dim.Count == 0) {
				// Пустое измерение → невозможно разрешить хоть какую-то комбинацию.
				return [];
			}
			dimensions.Add(dim);
		}

		// Имена ключей в порядке Fluent API — для нормализации insertion order финального словаря.
		var orderedKeyNames = ComputeOrderedKeyNames(stage, dimensions);

		var created = new List<StageInstance>();
		var workingMerged = new Dictionary<string, string>(StringComparer.Ordinal);
		Recurse(stage, dimensions, 0, workingMerged, orderedKeyNames, created);
		return created;
	}

	private void Recurse(
		StageDescriptor stage,
		List<List<IReadOnlyDictionary<string, string>>> dimensions,
		int dimIdx,
		Dictionary<string, string> current,
		IReadOnlyList<string> orderedKeyNames,
		List<StageInstance> output
	) {
		if (dimIdx == dimensions.Count) {
			// Все измерения совмещены — проверяем существование и зависимости.
			if (instances.Exists(stage.Name, current)) return;
			if (!DependencyResolver.AllDependenciesResolved(stage, current, instances, keyspace)) return;
			var ordered = NormalizeOrder(current, orderedKeyNames);
			output.Add(MaterializeInstance(stage, ordered));
			return;
		}

		foreach (var candidate in dimensions[dimIdx]) {
			// Пробуем добавить candidate к current, отслеживая добавленные ключи для отката.
			List<string>? rollback = null;
			bool incompatible = false;
			foreach (var kv in candidate) {
				if (current.TryGetValue(kv.Key, out var existing)) {
					if (!string.Equals(existing, kv.Value, StringComparison.Ordinal)) {
						incompatible = true;
						break;
					}
					// уже есть с тем же значением — пропускаем, не трогаем
				} else {
					current[kv.Key] = kv.Value;
					(rollback ??= []).Add(kv.Key);
				}
			}

			if (!incompatible) {
				Recurse(stage, dimensions, dimIdx + 1, current, orderedKeyNames, output);
			}

			// Откат всего, что добавили этим candidate-ом.
			if (rollback is not null) {
				foreach (var k in rollback) current.Remove(k);
			}
		}
	}

	private List<IReadOnlyDictionary<string, string>> ComputeDimension(StageDependency dep) {
		if (dep.Mode == DependencyMode.Instance) {
			return keyspace.Get(dep.TargetStageName)
				.Select(k => (IReadOnlyDictionary<string, string>)new Dictionary<string, string>(StringComparer.Ordinal) { [dep.TargetStageName] = k })
				.ToList();
		}
		return instances.InstancesOf(dep.TargetStageName)
			.Where(inst => inst.LastSuccess.HasValue)
			.Select(inst => inst.DependencyKeys)
			.ToList();
	}

	/// <summary>
	/// Вычисляет порядок имён ключей: проходим по stage.Dependencies в Fluent-порядке, для каждой dep
	/// добавляем имена компонентов, которые она вносит. Для DependsOnInstance это её собственное имя,
	/// для DependsOn — имена компонентов её candidate-dimensions (берём из первого candidate как пример,
	/// т. к. все элементы измерения имеют один и тот же набор ключей).
	/// </summary>
	private static List<string> ComputeOrderedKeyNames(
		StageDescriptor stage,
		List<List<IReadOnlyDictionary<string, string>>> dimensions
	) {
		var ordered = new List<string>();
		var seen = new HashSet<string>(StringComparer.Ordinal);
		for (int i = 0; i < stage.Dependencies.Count; i++) {
			var sample = dimensions[i].Count > 0 ? dimensions[i][0] : null;
			if (sample is null) continue;
			foreach (var kv in sample) {
				if (seen.Add(kv.Key)) ordered.Add(kv.Key);
			}
		}
		return ordered;
	}

	private static Dictionary<string, string> NormalizeOrder(
		Dictionary<string, string> merged,
		IReadOnlyList<string> orderedKeyNames
	) {
		var ordered = new Dictionary<string, string>(merged.Count, StringComparer.Ordinal);
		foreach (var name in orderedKeyNames) {
			if (merged.TryGetValue(name, out var value)) {
				ordered[name] = value;
			}
		}
		// Защита от ключей, которых нет в orderedKeyNames (теоретически невозможно, но safety).
		foreach (var kv in merged) {
			if (!ordered.ContainsKey(kv.Key)) ordered[kv.Key] = kv.Value;
		}
		return ordered;
	}

	private StageInstance MaterializeInstance(StageDescriptor stage, IReadOnlyDictionary<string, string> keys) {
		var fqn = DependencyKey.FormatFullyQualifiedName(stage.Name, keys);
		var encoded = DependencyKey.Encode(keys);
		var instance = new StageInstance {
			Stage = stage,
			DependencyKeys = keys,
			FullyQualifiedName = fqn,
			EncodedKey = encoded,
		};
		// NextAutoUtc = now → DueScanner подберёт инстанс при ближайшем проходе.
		instance.NextAutoUtc = time.GetUtcNow();
		instances.Add(instance);
		return instance;
	}
}
