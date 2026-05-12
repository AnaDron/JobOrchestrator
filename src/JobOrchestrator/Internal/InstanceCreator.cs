namespace JobOrchestrator.Internal;

/// <summary>
/// Создаёт инстансы стадий по мере разрешения зависимостей.
/// Алгоритм:
/// <list type="number">
/// <item>Для каждой зависимости стадии формируется «измерение» candidate-ключей.</item>
/// <item>Cartesian product над всеми измерениями.</item>
/// <item>Каждая комбинация merge-ится с проверкой совместимости (общие имена ключей → одинаковые значения).</item>
/// <item>Для каждой merged-комбинации проверяется разрешение всех зависимостей; если ок и инстанса ещё нет — создаётся.</item>
/// </list>
/// Insertion order ключей в результирующем словаре соответствует порядку зависимостей в Fluent API,
/// что обеспечивает стабильный FullyQualifiedName.
/// </summary>
internal sealed class InstanceCreator(JobManager jobs, KeyspaceRegistry keyspace) {
	private readonly JobManager _jobs = jobs;
	private readonly KeyspaceRegistry _keyspace = keyspace;

	public List<Job> EvaluateAndCreate(StageDescriptor stage) {
		List<Job> created = [];

		if (stage.Dependencies.Count == 0) {
			// Безключевая стадия → один инстанс с пустыми DependencyKeys.
			var empty = new Dictionary<string, string>(StringComparer.Ordinal);
			if (!_jobs.Exists(stage.Name, empty)) {
				created.Add(MaterializeInstance(stage, empty));
			}
			return created;
		}

		// Собираем измерения candidate-ключей.
		List<List<IReadOnlyDictionary<string, string>>> dimensions = new(stage.Dependencies.Count);
		foreach (var dep in stage.Dependencies) {
			var dim = ComputeDimension(dep);
			if (dim.Count == 0) {
				// Пустое измерение → cartesian product пуст → инстансов нет.
				return created;
			}
			dimensions.Add(dim);
		}

		foreach (var combo in CartesianProduct(dimensions)) {
			var merged = TryMergeOrdered(combo);
			if (merged is null) continue;
			if (_jobs.Exists(stage.Name, merged)) continue;
			if (!DependencyResolver.AllDependenciesResolved(stage, merged, _jobs, _keyspace)) continue;
			created.Add(MaterializeInstance(stage, merged));
		}
		return created;
	}

	private List<IReadOnlyDictionary<string, string>> ComputeDimension(StageDependency dep) {
		List<IReadOnlyDictionary<string, string>> result = [];
		if (dep.Mode == DependencyMode.Instance) {
			foreach (var k in _keyspace.Get(dep.TargetStageName)) {
				var single = new Dictionary<string, string>(StringComparer.Ordinal) { [dep.TargetStageName] = k };
				result.Add(single);
			}
		} else {
			foreach (var inst in _jobs.InstancesOf(dep.TargetStageName)) {
				if (inst.LastSuccess.HasValue) {
					result.Add(inst.DependencyKeys);
				}
			}
		}
		return result;
	}

	/// <summary>
	/// Merge словарей в порядке dimensions (= порядок Dependencies стадии = порядок Fluent API).
	/// Возвращает null при конфликте значений общего имени ключа.
	/// </summary>
	private static Dictionary<string, string>? TryMergeOrdered(IReadOnlyList<IReadOnlyDictionary<string, string>> combo) {
		Dictionary<string, string> result = new(StringComparer.Ordinal);
		foreach (var dict in combo) {
			foreach (var kv in dict) {
				if (result.TryGetValue(kv.Key, out var existing)) {
					if (!string.Equals(existing, kv.Value, StringComparison.Ordinal)) return null;
				} else {
					result[kv.Key] = kv.Value;
				}
			}
		}
		return result;
	}

	private Job MaterializeInstance(StageDescriptor stage, IReadOnlyDictionary<string, string> keys) {
		var fqn = DependencyKey.FormatFullyQualifiedName(stage.Name, keys);
		var encoded = DependencyKey.Encode(keys);
		var job = new Job {
			Stage = stage,
			DependencyKeys = keys,
			FullyQualifiedName = fqn,
			EncodedKey = encoded,
			NextTickAtMs = Environment.TickCount64,    // готов к немедленному запуску
		};
		_jobs.Add(job);
		return job;
	}

	private static IEnumerable<IReadOnlyList<IReadOnlyDictionary<string, string>>> CartesianProduct(
		List<List<IReadOnlyDictionary<string, string>>> dimensions
	) {
		if (dimensions.Count == 0) {
			yield return [];
			yield break;
		}
		int[] indices = new int[dimensions.Count];
		while (true) {
			IReadOnlyDictionary<string, string>[] combo = new IReadOnlyDictionary<string, string>[dimensions.Count];
			for (int i = 0; i < dimensions.Count; i++) {
				combo[i] = dimensions[i][indices[i]];
			}
			yield return combo;

			int j = dimensions.Count - 1;
			while (j >= 0) {
				indices[j]++;
				if (indices[j] < dimensions[j].Count) break;
				indices[j] = 0;
				j--;
			}
			if (j < 0) yield break;
		}
	}
}
