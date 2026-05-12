namespace JobOrchestrator.Internal;

/// <summary>
/// Граф стадий после сборки. Валидирует отсутствие циклов и висячих зависимостей.
/// Предоставляет helper'ы для каскада зависимостей и каскадного удаления.
/// </summary>
internal sealed class StageRegistry {
	private readonly Dictionary<string, StageDescriptor> _byName;

	public StageRegistry(IReadOnlyList<StageDescriptor> stages) {
		ArgumentNullException.ThrowIfNull(stages);
		_byName = new Dictionary<string, StageDescriptor>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) {
			if (!_byName.TryAdd(s.Name, s)) {
				throw new JobConfigurationException($"Дубль имени стадии: '{s.Name}'.");
			}
		}
		ValidateNoDanglingDependencies();
		ValidateNoCycles();
	}

	public StageDescriptor Get(string name) {
		if (!_byName.TryGetValue(name, out var s)) {
			throw new KeyNotFoundException($"Стадия '{name}' не найдена в реестре.");
		}
		return s;
	}

	public bool TryGet(string name, out StageDescriptor? descriptor) {
		bool ok = _byName.TryGetValue(name, out var s);
		descriptor = s;
		return ok;
	}

	public IReadOnlyCollection<StageDescriptor> AllStages => _byName.Values;

	/// <summary>Стадии, имеющие <c>DependsOnInstance(stageName)</c> в своих зависимостях.</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOnInstance(string stageName) {
		List<StageDescriptor> result = [];
		foreach (var s in _byName.Values) {
			foreach (var d in s.Dependencies) {
				if (d.Mode == DependencyMode.Instance && string.Equals(d.TargetStageName, stageName, StringComparison.Ordinal)) {
					result.Add(s);
					break;
				}
			}
		}
		return result;
	}

	/// <summary>Стадии, имеющие <c>DependsOn(stageName)</c> в своих зависимостях.</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOn(string stageName) {
		List<StageDescriptor> result = [];
		foreach (var s in _byName.Values) {
			foreach (var d in s.Dependencies) {
				if (d.Mode == DependencyMode.Whole && string.Equals(d.TargetStageName, stageName, StringComparison.Ordinal)) {
					result.Add(s);
					break;
				}
			}
		}
		return result;
	}

	/// <summary>
	/// Транзитивное замыкание стадий, инстансы которых могут унаследовать компонент ключа от <paramref name="stageName"/>.
	/// Используется при каскадном удалении: <c>RemoveKey(shops, uuid)</c> аффектирует productGroups, products, documents и т. д.
	/// </summary>
	public IReadOnlyList<StageDescriptor> StagesAffectedByKeyRemoval(string stageName) {
		HashSet<string> visited = new(StringComparer.Ordinal);
		Queue<string> queue = new();
		queue.Enqueue(stageName);
		while (queue.Count > 0) {
			string current = queue.Dequeue();
			foreach (var s in _byName.Values) {
				if (visited.Contains(s.Name)) continue;
				foreach (var d in s.Dependencies) {
					if (string.Equals(d.TargetStageName, current, StringComparison.Ordinal)) {
						visited.Add(s.Name);
						queue.Enqueue(s.Name);
						break;
					}
				}
			}
		}
		List<StageDescriptor> result = new(visited.Count);
		foreach (var name in visited) {
			result.Add(_byName[name]);
		}
		return result;
	}

	/// <summary>
	/// Топологический обратный порядок (листья перед корнями) для каскадной отмены.
	/// Применяется к подмножеству стадий, найденных через <see cref="StagesAffectedByKeyRemoval"/>.
	/// </summary>
	public IReadOnlyList<StageDescriptor> TopologicalSortReverse(IReadOnlyCollection<StageDescriptor> stages) {
		HashSet<string> stageNames = new(stages.Select(s => s.Name), StringComparer.Ordinal);
		HashSet<string> visited = new(StringComparer.Ordinal);
		List<StageDescriptor> order = [];
		foreach (var s in stages) {
			VisitForTopoSort(s, stageNames, visited, order);
		}
		// Стандартный topo-sort даёт «зависимости первыми». Reverse — «зависимости последними» (листья первыми).
		order.Reverse();
		return order;
	}

	private void VisitForTopoSort(StageDescriptor s, HashSet<string> filter, HashSet<string> visited, List<StageDescriptor> order) {
		if (!filter.Contains(s.Name)) return;
		if (!visited.Add(s.Name)) return;
		foreach (var d in s.Dependencies) {
			if (_byName.TryGetValue(d.TargetStageName, out var dep)) {
				VisitForTopoSort(dep, filter, visited, order);
			}
		}
		order.Add(s);
	}

	private void ValidateNoDanglingDependencies() {
		foreach (var s in _byName.Values) {
			foreach (var d in s.Dependencies) {
				if (!_byName.ContainsKey(d.TargetStageName)) {
					throw new JobConfigurationException(
						$"Стадия '{s.Name}' зависит от несуществующей стадии '{d.TargetStageName}'.");
				}
			}
		}
	}

	private void ValidateNoCycles() {
		foreach (var s in _byName.Values) {
			HashSet<string> visited = new(StringComparer.Ordinal);
			Stack<string> path = new();
			DfsCheck(s.Name, visited, path);
		}
	}

	private void DfsCheck(string node, HashSet<string> visited, Stack<string> path) {
		if (path.Contains(node)) {
			List<string> cycle = [.. path.Reverse(), node];
			throw new JobConfigurationException(
				$"Цикл в графе зависимостей: {string.Join(" -> ", cycle)}");
		}
		if (!visited.Add(node)) return;
		path.Push(node);
		if (_byName.TryGetValue(node, out var desc)) {
			foreach (var d in desc.Dependencies) {
				DfsCheck(d.TargetStageName, visited, path);
			}
		}
		path.Pop();
	}
}
