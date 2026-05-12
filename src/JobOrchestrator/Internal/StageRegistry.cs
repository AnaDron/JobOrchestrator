namespace JobOrchestrator.Internal;

/// <summary>
/// Граф стадий после сборки. Валидирует отсутствие циклов и висячих зависимостей.
/// Предоставляет helper'ы для каскада зависимостей и каскадного удаления.
/// </summary>
internal sealed class StageRegistry(IReadOnlyList<StageDescriptor> stages) {
	private readonly Dictionary<string, StageDescriptor> _byName = Build(stages);

	private static Dictionary<string, StageDescriptor> Build(IReadOnlyList<StageDescriptor> stages) {
		ArgumentNullException.ThrowIfNull(stages);
		var dict = new Dictionary<string, StageDescriptor>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) {
			if (!dict.TryAdd(s.Name, s))
				throw new JobConfigurationException($"Дубль имени стадии: '{s.Name}'.");
		}
		ValidateNoDanglingDependencies(dict);
		ValidateNoCycles(dict);
		return dict;
	}

	public StageDescriptor Get(string name) =>
		_byName.TryGetValue(name, out var s)
			? s
			: throw new KeyNotFoundException($"Стадия '{name}' не найдена в реестре.");

	public bool TryGet(string name, out StageDescriptor? descriptor) =>
		_byName.TryGetValue(name, out descriptor);

	public IReadOnlyCollection<StageDescriptor> AllStages => _byName.Values;

	/// <summary>Стадии, имеющие <c>DependsOnInstance(stageName)</c> в своих зависимостях.</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOnInstance(string stageName) {
		var result = _byName.Values
			.Where(s => s.Dependencies.Any(d =>
				d.Mode == DependencyMode.Instance &&
				string.Equals(d.TargetStageName, stageName, StringComparison.Ordinal)))
			.ToList();
		return result.Count == 0 ? [] : result;
	}

	/// <summary>Стадии, имеющие <c>DependsOn(stageName)</c> в своих зависимостях.</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOn(string stageName) {
		var result = _byName.Values
			.Where(s => s.Dependencies.Any(d =>
				d.Mode == DependencyMode.Whole &&
				string.Equals(d.TargetStageName, stageName, StringComparison.Ordinal)))
			.ToList();
		return result.Count == 0 ? [] : result;
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
			foreach (var s in _byName.Values.Where(s =>
				!visited.Contains(s.Name) &&
				s.Dependencies.Any(d => string.Equals(d.TargetStageName, current, StringComparison.Ordinal)))) {
				visited.Add(s.Name);
				queue.Enqueue(s.Name);
			}
		}
		return visited.Select(name => _byName[name]).ToList();
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

	private static void ValidateNoDanglingDependencies(Dictionary<string, StageDescriptor> byName) {
		foreach (var s in byName.Values)
			foreach (var d in s.Dependencies.Where(d => !byName.ContainsKey(d.TargetStageName)))
				throw new JobConfigurationException(
					$"Стадия '{s.Name}' зависит от несуществующей стадии '{d.TargetStageName}'.");
	}

	private static void ValidateNoCycles(Dictionary<string, StageDescriptor> byName) {
		// Триколорный DFS: white (не посещён), gray (в стеке), black (завершён).
		// Один проход по всем вершинам — O(V+E).
		HashSet<string> gray = new(StringComparer.Ordinal);
		HashSet<string> black = new(StringComparer.Ordinal);
		foreach (var s in byName.Values)
			DfsCheck(s.Name, byName, gray, black, []);
	}

	private static void DfsCheck(
		string node,
		Dictionary<string, StageDescriptor> byName,
		HashSet<string> gray,
		HashSet<string> black,
		List<string> path
	) {
		if (black.Contains(node)) return;
		if (!gray.Add(node)) {
			// Нашли обратное ребро — цикл. Выделяем его из path.
			int cycleStart = path.IndexOf(node);
			List<string> cycle = [.. path[cycleStart..], node];
			throw new JobConfigurationException(
				$"Цикл в графе зависимостей: {string.Join(" -> ", cycle)}");
		}
		path.Add(node);
		if (byName.TryGetValue(node, out var desc)) {
			foreach (var d in desc.Dependencies)
				DfsCheck(d.TargetStageName, byName, gray, black, path);
		}
		path.RemoveAt(path.Count - 1);
		gray.Remove(node);
		black.Add(node);
	}
}
