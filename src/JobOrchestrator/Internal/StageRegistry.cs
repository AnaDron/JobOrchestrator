namespace JobOrchestrator.Internal;

/// <summary>
/// By-name индекс <see cref="StageDescriptor"/>-ов. Конструктор берёт bare-дескрипторы (только raw
/// user-fields) и raw-deps-граф, вычисляет computed-поля (transitive ExpectedKeyNames, reverse-индексы,
/// AffectedByKeyRemoval, CancellationRank) и заполняет дескрипторы in-place. После выхода из ctor'а
/// все дескрипторы fully-initialized.
/// <para>
/// <b>Контракт:</b> предполагает граф уже валидированным (циклы / dangling-deps проверены
/// <see cref="Configuration.Internal.ConfigurationValidator"/> до этого момента). Recursive compute
/// для <c>ExpectedKeyNames</c> / <c>CancellationRank</c> уйдёт в бесконечную рекурсию на циклах.
/// </para>
/// </summary>
internal sealed class StageRegistry {
	private readonly Dictionary<string, StageDescriptor> _byName;

	/// <summary>Глобальный лимит параллельных итераций из <see cref="Configuration.JobDefaults.GlobalConcurrencyLimit"/>.</summary>
	public int? GlobalConcurrencyLimit { get; }

	public StageRegistry(IReadOnlyList<StageDescriptor> stages, int? globalConcurrencyLimit = null)
		: this(stages, BuildEmptyDeps(stages), globalConcurrencyLimit) { }

	public StageRegistry(
		IReadOnlyList<StageDescriptor> stages,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps,
		int? globalConcurrencyLimit = null
	) {
		ArgumentNullException.ThrowIfNull(stages);
		ArgumentNullException.ThrowIfNull(rawDeps);
		GlobalConcurrencyLimit = globalConcurrencyLimit;
		_byName = new Dictionary<string, StageDescriptor>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) {
			if (!_byName.TryAdd(s.Name, s)) {
				throw new JobConfigurationException($"Дубль имени стадии: '{s.Name}'.");
			}
		}
		FinalizeDescriptors(rawDeps);
	}

	public StageDescriptor Get(string name) =>
		_byName.TryGetValue(name, out var s)
			? s
			: throw new KeyNotFoundException($"Стадия '{name}' не найдена в реестре.");

	public IReadOnlyCollection<StageDescriptor> AllStages => _byName.Values;

	private static Dictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> BuildEmptyDeps(
		IReadOnlyList<StageDescriptor> stages
	) {
		var dict = new Dictionary<string, IReadOnlyList<(string, DependencyMode)>>(stages.Count, StringComparer.Ordinal);
		foreach (var s in stages) dict[s.Name] = [];
		return dict;
	}

	private void FinalizeDescriptors(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var deps = ComputeDependencies(rawDeps);
		var expected = ComputeExpectedKeyNames(rawDeps);
		var (dependentsWhole, dependentsInstance) = ComputeDependents(rawDeps);
		var affected = ComputeAffectedByKeyRemoval(rawDeps);
		var rank = ComputeCancellationRank(rawDeps);

		foreach (var d in _byName.Values) {
			d.Dependencies = deps.TryGetValue(d.Name, out var dv) ? dv : [];
			d.ExpectedKeyNames = expected.TryGetValue(d.Name, out var ev) ? ev : [];
			d.DependentsWhole = dependentsWhole.TryGetValue(d.Name, out var dw) ? dw : [];
			d.DependentsInstance = dependentsInstance.TryGetValue(d.Name, out var di) ? di : [];
			d.AffectedByKeyRemoval = affected.TryGetValue(d.Name, out var av) ? av : [];
			d.CancellationRank = rank.TryGetValue(d.Name, out var rv) ? rv : 0;
		}
	}

	private Dictionary<string, IReadOnlyList<StageDependency>> ComputeDependencies(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var result = new Dictionary<string, IReadOnlyList<StageDependency>>(rawDeps.Count, StringComparer.Ordinal);
		foreach (var kv in rawDeps) {
			var list = new List<StageDependency>(kv.Value.Count);
			foreach (var rd in kv.Value) {
				list.Add(new StageDependency(Get(rd.TargetName), rd.Mode));
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

	private (
		Dictionary<string, IReadOnlyList<StageDescriptor>> Whole,
		Dictionary<string, IReadOnlyList<StageDescriptor>> Instance
	) ComputeDependents(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		var whole = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		var instance = new Dictionary<string, List<StageDescriptor>>(StringComparer.Ordinal);
		foreach (var kv in rawDeps) {
			var dependent = Get(kv.Key);
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
	private Dictionary<string, IReadOnlyList<StageDescriptor>> ComputeAffectedByKeyRemoval(
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
						closure.Add(Get(child));
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
