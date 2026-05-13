namespace JobOrchestrator.Internal;

/// <summary>
/// Граф стадий после сборки. Валидирует отсутствие циклов и висячих зависимостей.
/// <para>
/// Все обратные индексы (<c>StagesDependingOn</c>, <c>StagesDependingOnInstance</c>,
/// <c>StagesAffectedByKeyRemoval</c>) <b>pre-computed</b> в конструкторе — O(V+E) суммарно один раз
/// при старте процесса. Каждый последующий lookup — O(1) поиск в словаре, без linq-аллокаций
/// на горячем пути event loop'а. Аналогично <see cref="TopologicalOrderForCascade"/> кэшируется
/// глобально, а каскадная фильтрация — простая проекция по pre-computed rank-у.
/// </para>
/// </summary>
internal sealed class StageRegistry {
	private readonly Dictionary<string, StageDescriptor> _byName;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _dependingOnWhole;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _dependingOnInstance;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _affectedByKeyRemoval;
	private readonly Dictionary<string, int> _cancellationRank;

	public StageRegistry(IReadOnlyList<StageDescriptor> stages) {
		ArgumentNullException.ThrowIfNull(stages);
		_byName = new Dictionary<string, StageDescriptor>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) {
			if (!_byName.TryAdd(s.Name, s))
				throw new JobConfigurationException($"Дубль имени стадии: '{s.Name}'.");
		}
		ValidateNoDanglingDependencies(_byName);
		ValidateNoCycles(_byName);

		_dependingOnWhole = BuildReverseIndex(_byName, DependencyMode.Whole);
		_dependingOnInstance = BuildReverseIndex(_byName, DependencyMode.Instance);
		_affectedByKeyRemoval = BuildAffectedByKeyRemoval(_byName);
		_cancellationRank = BuildCancellationRank(_byName);
	}

	public StageDescriptor Get(string name) =>
		_byName.TryGetValue(name, out var s)
			? s
			: throw new KeyNotFoundException($"Стадия '{name}' не найдена в реестре.");

	public bool TryGet(string name, out StageDescriptor? descriptor) =>
		_byName.TryGetValue(name, out descriptor);

	public IReadOnlyCollection<StageDescriptor> AllStages => _byName.Values;

	/// <summary>Стадии, имеющие <c>DependsOnInstance(stageName)</c> в своих зависимостях. O(1).</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOnInstance(string stageName) =>
		_dependingOnInstance.TryGetValue(stageName, out var list) ? list : [];

	/// <summary>Стадии, имеющие <c>DependsOn(stageName)</c> в своих зависимостях. O(1).</summary>
	public IReadOnlyList<StageDescriptor> StagesDependingOn(string stageName) =>
		_dependingOnWhole.TryGetValue(stageName, out var list) ? list : [];

	/// <summary>
	/// Транзитивное замыкание стадий, инстансы которых могут унаследовать компонент ключа от <paramref name="stageName"/>.
	/// O(1) — pre-computed BFS.
	/// </summary>
	public IReadOnlyList<StageDescriptor> StagesAffectedByKeyRemoval(string stageName) =>
		_affectedByKeyRemoval.TryGetValue(stageName, out var list) ? list : [];

	/// <summary>
	/// Глобальный ранг стадии для каскадной отмены: чем меньше ранг — тем «ближе к листу» (cancel-ить первым).
	/// Pre-computed один раз: листья (стадии без зависимых-вниз) получают ранг 0, корни — наибольший.
	/// При cascade-removal достаточно отсортировать аффектированные инстансы по этому рангу — без локального
	/// topo-sort-а каждый раз.
	/// </summary>
	public int CancellationRank(string stageName) =>
		_cancellationRank.TryGetValue(stageName, out var rank) ? rank : 0;

	private static Dictionary<string, IReadOnlyList<StageDescriptor>> BuildReverseIndex(
		Dictionary<string, StageDescriptor> byName,
		DependencyMode mode
	) {
		var temp = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		foreach (var s in byName.Values) {
			foreach (var dep in s.Dependencies) {
				if (dep.Mode != mode) continue;
				if (!temp.TryGetValue(dep.TargetStageName, out var list)) {
					list = [];
					temp[dep.TargetStageName] = list;
				}
				list.Add(s);
			}
		}
		var result = new Dictionary<string, IReadOnlyList<StageDescriptor>>(temp.Count, StringComparer.Ordinal);
		foreach (var kv in temp) result[kv.Key] = kv.Value;
		return result;
	}

	private static Dictionary<string, IReadOnlyList<StageDescriptor>> BuildAffectedByKeyRemoval(
		Dictionary<string, StageDescriptor> byName
	) {
		// Forward adjacency: stageName → стадии, которые зависят от него любым способом.
		var forward = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		foreach (var s in byName.Values) {
			foreach (var dep in s.Dependencies) {
				if (!forward.TryGetValue(dep.TargetStageName, out var list)) {
					list = [];
					forward[dep.TargetStageName] = list;
				}
				list.Add(s);
			}
		}

		var result = new Dictionary<string, IReadOnlyList<StageDescriptor>>(byName.Count, StringComparer.Ordinal);
		foreach (var root in byName.Values) {
			HashSet<string> visited = new(StringComparer.Ordinal);
			List<StageDescriptor> closure = [];
			Queue<string> queue = new();
			queue.Enqueue(root.Name);
			while (queue.Count > 0) {
				var current = queue.Dequeue();
				if (!forward.TryGetValue(current, out var children)) continue;
				foreach (var child in children) {
					if (visited.Add(child.Name)) {
						closure.Add(child);
						queue.Enqueue(child.Name);
					}
				}
			}
			result[root.Name] = closure;
		}
		return result;
	}

	/// <summary>
	/// Глубина «вниз по графу зависимостей» для каждой стадии. Лист = 0, корень = max.
	/// Используется как порядок cancellation (сначала листья).
	/// </summary>
	private static Dictionary<string, int> BuildCancellationRank(Dictionary<string, StageDescriptor> byName) {
		// Forward adjacency: stageName → стадии, которые зависят от него.
		var forward = new Dictionary<string, List<string>>(StringComparer.Ordinal);
		foreach (var s in byName.Values) {
			forward[s.Name] = [];
		}
		foreach (var s in byName.Values) {
			foreach (var dep in s.Dependencies) {
				if (forward.TryGetValue(dep.TargetStageName, out var list)) list.Add(s.Name);
			}
		}

		// rank[x] = 1 + max(rank[y] для всех y, зависящих от x); лист → 0.
		var rank = new Dictionary<string, int>(byName.Count, StringComparer.Ordinal);
		foreach (var name in byName.Keys) Compute(name);
		return rank;

		int Compute(string node) {
			if (rank.TryGetValue(node, out var cached)) return cached;
			int max = 0;
			foreach (var child in forward[node]) {
				int childRank = Compute(child) + 1;
				if (childRank > max) max = childRank;
			}
			rank[node] = max;
			return max;
		}
	}

	private static void ValidateNoDanglingDependencies(Dictionary<string, StageDescriptor> byName) {
		foreach (var s in byName.Values)
			foreach (var d in s.Dependencies.Where(d => !byName.ContainsKey(d.TargetStageName)))
				throw new JobConfigurationException(
					$"Стадия '{s.Name}' зависит от несуществующей стадии '{d.TargetStageName}'.");
	}

	private static void ValidateNoCycles(Dictionary<string, StageDescriptor> byName) {
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
