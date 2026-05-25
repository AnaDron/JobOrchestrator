using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Backtracking-merge матчер для создания инстансов стадий по мере разрешения зависимостей.
/// Stateless: вся «память» — параметры (<see cref="InstanceManager"/>, channel, time) — поэтому
/// реализация живёт в <c>static</c>-методах partial-секции <see cref="EventLoop"/>.
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
/// что критично для графов с общим ключом через все зависимости (например, shops=u_i проходит сквозь весь граф).
/// </para>
/// <para>
/// Insertion order компонентов в результирующем словаре нормализуется по
/// <see cref="StageDescriptor.ExpectedKeyNames"/> (= порядок Fluent API объявлений транзитивно),
/// что обеспечивает стабильный FullyQualifiedName независимо от того, в каком порядке прилетали
/// разрешающие события.
/// </para>
/// </summary>
internal sealed partial class EventLoop {
	/// <summary>
	/// Вычисляет и материализует все инстансы стадии, чьи зависимости разрешены. Каждый созданный
	/// инстанс уже добавлен в <paramref name="instances"/>; caller отвечает за notify-фазу
	/// (<see cref="JobOrchestratorRuntime.NotifyInstanceAdded"/>) и логирование.
	/// </summary>
	internal static List<Instance> EvaluateAndCreate(
		StageDescriptor stage,
		InstanceManager instances,
		Channel<OrchestratorEvent> channel,
		TimeProvider time
	) {
		if (stage.Dependencies.Count == 0) {
			// Безключевая стадия → один инстанс с пустыми DependencyKeys.
			var emptyId = new InstanceIdentity(stage);
			return instances.Exists(emptyId) ? [] : [MaterializeInstance(emptyId, instances, channel, time)];
		}

		// Собираем измерения candidate-ключей.
		var dimensions = new List<List<IReadOnlyDictionary<string, string>>>(stage.Dependencies.Count);
		foreach (var dep in stage.Dependencies) {
			var dim = ComputeDimension(dep, instances);
			if (dim.Count == 0) {
				// Пустое измерение → невозможно разрешить хоть какую-то комбинацию.
				return [];
			}

			dimensions.Add(dim);
		}

		var created = new List<Instance>();
		var workingMerged = new Dictionary<string, string>(StringComparer.Ordinal);
		Recurse(stage, dimensions, 0, workingMerged, created, instances, channel, time);
		return created;
	}

	private static void Recurse(
		StageDescriptor stage,
		List<List<IReadOnlyDictionary<string, string>>> dimensions,
		int dimIdx,
		Dictionary<string, string> current,
		List<Instance> output,
		InstanceManager instances,
		Channel<OrchestratorEvent> channel,
		TimeProvider time
	) {
		if (dimIdx == dimensions.Count) {
			// Все измерения совмещены — проверяем существование и зависимости.
			var identity = new InstanceIdentity(stage, current);
			if (instances.Exists(identity)) return;
			if (!AllDependenciesResolved(stage, current, instances)) return;
			output.Add(MaterializeInstance(identity, instances, channel, time));
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
				Recurse(stage, dimensions, dimIdx + 1, current, output, instances, channel, time);
			}

			// Откат всего, что добавили этим candidate-ом.
			if (rollback is not null) {
				foreach (var k in rollback) current.Remove(k);
			}
		}
	}

	/// <summary>
	/// Predicate финального шага <see cref="Recurse"/>: все зависимости стадии разрешены для
	/// child-кандидата с compound-ключами <paramref name="childKeys"/>.
	/// <para>
	/// <b>Инвариант, обеспечиваемый Recurse:</b> <paramref name="childKeys"/> — это компонованные ключи
	/// потенциального child-инстанса, накопленные через все измерения. Каждый родитель имеет проекцию
	/// своих <see cref="Instance.DependencyKeys"/> на это множество (child ⊇ parent по дизайну
	/// backtracking-merge). Поэтому subset-check ниже — fast-path для уникального paired-родителя.
	/// </para>
	/// <list type="bullet">
	/// <item>
	/// <b><see cref="DependencyMode.Whole"/> (<c>DependsOn</c>):</b> child наследует ключи только из
	/// родителя, у которого был хотя бы один успешный цикл — <c>LastSuccess != null</c>.
	/// </item>
	/// <item>
	/// <b><see cref="DependencyMode.Instance"/> (<c>DependsOnInstance</c>):</b> reactive — родитель
	/// существует, и эмитнутый ключ есть в его <see cref="Instance.EmittedKeys"/>. <c>LastSuccess</c>
	/// не требуется (long-running emitter: child материализуется немедленно при <c>AddKey</c>).
	/// </item>
	/// </list>
	/// </summary>
	private static bool AllDependenciesResolved(
		StageDescriptor stage,
		IReadOnlyDictionary<string, string> childKeys,
		InstanceManager instances
	) {
		foreach (var dep in stage.Dependencies) {
			// Парный parent — тот, чьи DependencyKeys ⊆ childKeys. Инвариант backtracking-merge
			// гарантирует уникальность; первый match — он же единственный.
			Instance? parent = null;
			foreach (var candidate in instances.InstancesOf(dep.Target)) {
				bool projectionMatches = true;
				foreach (var kv in candidate.DependencyKeys) {
					if (!childKeys.TryGetValue(kv.Key, out var v) || !string.Equals(v, kv.Value, StringComparison.Ordinal)) {
						projectionMatches = false;
						break;
					}
				}
				if (projectionMatches) {
					parent = candidate;
					break;
				}
			}
			if (parent is null) return false;

			if (dep.Mode == DependencyMode.Whole) {
				if (parent.Metrics.Stats.LastSuccess is null) return false;
			} else {
				// Per-emitter keyspace-check защищает от race «KeyAdded → KeyRemoved».
				if (!childKeys.TryGetValue(dep.Target.Name, out var key) || !parent.ContainsEmittedKey(key)) return false;
			}
		}
		return true;
	}

	private static List<IReadOnlyDictionary<string, string>> ComputeDimension(
		StageDependency dep,
		InstanceManager instances
	) {
		var result = new List<IReadOnlyDictionary<string, string>>();

		switch (dep.Mode) {
		case DependencyMode.Instance:
			// Per-emitter keyspace живёт на самом инстансе (Instance.EmittedKeys).
			// Candidate = emitter's DependencyKeys ∪ { Target.Name: k } per каждый k в emittedKeys.
			// Это корректно поддерживает multi-instance-эмитеров, поскольку каждый эмитер вносит ТОЛЬКО
			// свои ключи (а не глобальный пул всех ключей стадии).
			foreach (var emitter in instances.InstancesOf(dep.Target)) {
				if (emitter.EmittedKeys.Count == 0) continue;
				var emitterKeys = emitter.DependencyKeys;
				foreach (var key in emitter.EmittedKeys) {
					var combined = new Dictionary<string, string>(emitterKeys.Count + 1, StringComparer.Ordinal);
					foreach (var ek in emitterKeys) combined[ek.Key] = ek.Value;
					combined[dep.Target.Name] = key;
					result.Add(combined);
				}
			}

			break;
		case DependencyMode.Whole:
			foreach (var inst in instances.InstancesOf(dep.Target)) {
				if (inst.Metrics.Stats.LastSuccess.HasValue) result.Add(inst.DependencyKeys);
			}

			break;
		default:
			throw new ArgumentOutOfRangeException(nameof(dep), dep.Mode, $"Неизвестный режим зависимости: {dep.Mode}.");
		}

		return result;
	}

	private static Instance MaterializeInstance(
		InstanceIdentity identity,
		InstanceManager instances,
		Channel<OrchestratorEvent> channel,
		TimeProvider time
	) {
		var instance = new Instance { Identity = identity };
		// Pre-allocated Sink: один объект на lifetime инстанса (Source/Writer постоянны), переиспользуется
		// всеми итерациями StageRunner-а — экономим аллокацию per-iteration.
		instance.Sink = new ChannelJobContextSink(channel.Writer, instance);
		// NextAutoUtc = now → DueScanner подберёт инстанс при ближайшем проходе.
		instance.SetMetrics(JobMetrics.Empty.WithNextAutoUtc(time.GetUtcNow()));
		instances.Add(instance);
		return instance;
	}
}
