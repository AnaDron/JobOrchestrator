namespace JobOrchestrator.Internal;

/// <summary>
/// Pre-computes все computed-поля для всех дескрипторов один раз в ctor. После этого
/// <c>*Of(stage)</c>-методы — O(1) lookups по by-name мапам.
/// <para>
/// <b>Контракт:</b> предполагает, что граф уже валиден (циклы / dangling-deps проверены
/// <see cref="Configuration.Internal.ConfigurationValidator"/> до этого момента). Если кто-то
/// сконструирует Initializer на невалидном графе — может уйти в бесконечную рекурсию в
/// <c>ComputeExpectedKeyNames</c>. Single-call-site инвариант, не дублируем валидацию.
/// </para>
/// </summary>
internal sealed class StageInitializer : IStageInitializer {
	private readonly Dictionary<string, IReadOnlyList<StageDependency>> _deps;
	private readonly Dictionary<string, IReadOnlyList<string>> _expected;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _dependentsWhole;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _dependentsInstance;
	private readonly Dictionary<string, IReadOnlyList<StageDescriptor>> _affected;
	private readonly Dictionary<string, int> _rank;

	public StageInitializer(
		StageRegistry registry,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		ArgumentNullException.ThrowIfNull(registry);
		ArgumentNullException.ThrowIfNull(rawDeps);
		_deps = ComputeDependencies(registry, rawDeps);
		_expected = ComputeExpectedKeyNames(rawDeps);
		(_dependentsWhole, _dependentsInstance) = ComputeDependents(registry, rawDeps);
		_affected = ComputeAffectedByKeyRemoval(registry, rawDeps);
		_rank = ComputeCancellationRank(rawDeps);
	}

	public IReadOnlyList<StageDependency> DependenciesOf(StageDescriptor s) => _deps[s.Name];
	public IReadOnlyList<string> ExpectedKeyNamesOf(StageDescriptor s) => _expected[s.Name];
	public IReadOnlyList<StageDescriptor> DependentsWholeOf(StageDescriptor s) =>
		_dependentsWhole.TryGetValue(s.Name, out var v) ? v : [];
	public IReadOnlyList<StageDescriptor> DependentsInstanceOf(StageDescriptor s) =>
		_dependentsInstance.TryGetValue(s.Name, out var v) ? v : [];
	public IReadOnlyList<StageDescriptor> AffectedByKeyRemovalOf(StageDescriptor s) =>
		_affected.TryGetValue(s.Name, out var v) ? v : [];
	public int CancellationRankOf(StageDescriptor s) => _rank[s.Name];

	private static Dictionary<string, IReadOnlyList<StageDependency>> ComputeDependencies(
		StageRegistry registry,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var result = new Dictionary<string, IReadOnlyList<StageDependency>>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var kv in rawDeps) {
			var list = new List<StageDependency>(kv.Value.Count);
			foreach (var rd in kv.Value) {
				list.Add(new StageDependency(registry.Get(rd.TargetName), rd.Mode));
			}
			result[kv.Key] = list;
		}
		return result;
	}

	/// <summary>
	/// Транзитивно собирает имена ожидаемых key-измерений: для каждой стадии — наследованные через
	/// <c>DependsOn</c>-цепочку имена parent.ExpectedKeyNames; для <c>DependsOnInstance(parent)</c>
	/// дополнительно добавляет имя самого parent. Recursive memo per name.
	/// </summary>
	private static Dictionary<string, IReadOnlyList<string>> ComputeExpectedKeyNames(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var memo = new Dictionary<string, IReadOnlyList<string>>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var name in rawDeps.Keys) _ = Collect(name);
		return memo;

		IReadOnlyList<string> Collect(string name) {
			if (memo.TryGetValue(name, out var cached)) return cached;
			var names = new List<string>();
			var seen = new HashSet<string>(StringComparer.Ordinal);
			if (rawDeps.TryGetValue(name, out var deps)) {
				foreach (var dep in deps) {
					foreach (var inherited in Collect(dep.TargetName)) {
						if (seen.Add(inherited)) names.Add(inherited);
					}
					if (dep.Mode == DependencyMode.Instance && seen.Add(dep.TargetName)) {
						names.Add(dep.TargetName);
					}
				}
			}
			memo[name] = names;
			return names;
		}
	}

	private static (
		Dictionary<string, IReadOnlyList<StageDescriptor>> Whole,
		Dictionary<string, IReadOnlyList<StageDescriptor>> Instance
	) ComputeDependents(
		StageRegistry registry,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var whole = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		var instance = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		foreach (var kv in rawDeps) {
			var dependent = registry.Get(kv.Key);
			foreach (var rd in kv.Value) {
				var map = rd.Mode == DependencyMode.Whole ? whole : instance;
				if (!map.TryGetValue(rd.TargetName, out var list)) {
					list = [];
					map[rd.TargetName] = list;
				}
				list.Add(dependent);
			}
		}
		return (
			whole.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<StageDescriptor>)kv.Value, StringComparer.Ordinal),
			instance.ToDictionary(kv => kv.Key, kv => (IReadOnlyList<StageDescriptor>)kv.Value, StringComparer.Ordinal)
		);
	}

	/// <summary>
	/// Транзитивное замыкание стадий вниз по графу: BFS по «forward»-направлению зависимостей
	/// (parent → dependent) через объединение whole+instance.
	/// </summary>
	private static Dictionary<string, IReadOnlyList<StageDescriptor>> ComputeAffectedByKeyRemoval(
		StageRegistry registry,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var forward = BuildForwardAdjacency(rawDeps);
		var result = new Dictionary<string, IReadOnlyList<StageDescriptor>>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var rootName in rawDeps.Keys) {
			var visited = new HashSet<string>(StringComparer.Ordinal);
			var closure = new List<StageDescriptor>();
			var queue = new Queue<string>();
			queue.Enqueue(rootName);
			while (queue.Count > 0) {
				var current = queue.Dequeue();
				foreach (var child in forward[current]) {
					if (visited.Add(child)) {
						closure.Add(registry.Get(child));
						queue.Enqueue(child);
					}
				}
			}
			result[rootName] = closure;
		}
		return result;
	}

	/// <summary>
	/// Ранг = 1 + max(rank[y]) для всех y, зависящих от x; лист (без dependents) → 0.
	/// </summary>
	private static Dictionary<string, int> ComputeCancellationRank(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var forward = BuildForwardAdjacency(rawDeps);
		var rank = new Dictionary<string, int>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var name in rawDeps.Keys) _ = Compute(name);
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

	/// <summary>
	/// Строит forward-граф: parentName → список имён стадий, зависящих от него (любым режимом).
	/// Все ключи rawDeps предварительно инициализированы пустыми списками, поэтому прямой доступ
	/// по индексатору безопасен для любого известного имени стадии.
	/// </summary>
	private static Dictionary<string, List<string>> BuildForwardAdjacency(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var forward = new Dictionary<string, List<string>>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var name in rawDeps.Keys) forward[name] = [];
		foreach (var (stageName, deps) in rawDeps) {
			foreach (var (targetName, _) in deps) {
				if (forward.TryGetValue(targetName, out var list)) list.Add(stageName);
			}
		}
		return forward;
	}
}
