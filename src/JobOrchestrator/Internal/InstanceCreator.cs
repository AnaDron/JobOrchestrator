using System.Threading.Channels;

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
/// Insertion order компонентов в результирующем словаре нормализуется по
/// <see cref="StageDescriptor.ExpectedKeyNames"/> (= порядок Fluent API объявлений транзитивно),
/// что обеспечивает стабильный FullyQualifiedName независимо от того, в каком порядке прилетали
/// разрешающие события.
/// </para>
/// </summary>
internal sealed class InstanceCreator(
	InstanceManager instances,
	KeyspaceRegistry keyspace,
	TimeProvider time,
	Channel<OrchestratorEvent> channel
) {
	public List<Instance> EvaluateAndCreate(StageDescriptor stage) {
		if (stage.Dependencies.Count == 0) {
			// Безключевая стадия → один инстанс с пустыми DependencyKeys.
			var emptyId = new InstanceIdentity(stage);
			return instances.Exists(emptyId) ? [] : [MaterializeInstance(emptyId)];
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

		var created = new List<Instance>();
		var workingMerged = new Dictionary<string, string>(StringComparer.Ordinal);
		Recurse(stage, dimensions, 0, workingMerged, created);
		return created;
	}

	private void Recurse(
		StageDescriptor stage,
		List<List<IReadOnlyDictionary<string, string>>> dimensions,
		int dimIdx,
		Dictionary<string, string> current,
		List<Instance> output
	) {
		if (dimIdx == dimensions.Count) {
			// Все измерения совмещены — проверяем существование и зависимости.
			var identity = new InstanceIdentity(stage, current);
			if (instances.Exists(identity)) return;
			if (!DependencyResolver.AllDependenciesResolved(stage, current, instances, keyspace)) return;
			output.Add(MaterializeInstance(identity));
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
				Recurse(stage, dimensions, dimIdx + 1, current, output);
			}

			// Откат всего, что добавили этим candidate-ом.
			if (rollback is not null) {
				foreach (var k in rollback) current.Remove(k);
			}
		}
	}

	private List<IReadOnlyDictionary<string, string>> ComputeDimension(StageDependency dep) {
		if (dep.Mode == DependencyMode.Instance) {
			// Per-emitter buckets: для каждого инстанса-эмитера X.bucket даёт пары (emitterKeys, keys).
			// Candidate = emitter's keys ∪ { Target.Name: k } per каждый k в bucket.Keys.
			// Это корректно поддерживает multi-instance-эмитеров, поскольку каждый эмитер вносит ТОЛЬКО
			// свои ключи (а не глобальный пул всех ключей стадии).
			var result = new List<IReadOnlyDictionary<string, string>>();
			foreach (var bucket in keyspace.SnapshotByStage(dep.Target)) {
				foreach (var key in bucket.Keys) {
					var emitterKeys = bucket.Emitter.DependencyKeys;
					var combined = new Dictionary<string, string>(emitterKeys.Count + 1, StringComparer.Ordinal);
					foreach (var ek in emitterKeys) combined[ek.Key] = ek.Value;
					combined[dep.Target.Name] = key;
					result.Add(combined);
				}
			}
			return result;
		}
		var result = new List<IReadOnlyDictionary<string, string>>();
		foreach (var inst in instances.InstancesOf(dep.Target)) {
			if (inst.Metrics.LastSuccess.HasValue) result.Add(inst.DependencyKeys);
		}
		return result;
	}

	private Instance MaterializeInstance(InstanceIdentity identity) {
		var instance = new Instance { Identity = identity };
		// Pre-allocated Sink: один объект на lifetime инстанса (Source/Writer постоянны), переиспользуется
		// всеми итерациями StageRunner-а — экономим аллокацию per-iteration.
		instance.Sink = new ChannelJobContextSink(channel.Writer, instance);
		// NextAutoUtc = now → DueScanner подберёт инстанс при ближайшем проходе.
		instance.SetMetrics(JobMetrics.Empty with { NextAutoUtc = time.GetUtcNow() });
		instances.Add(instance);
		return instance;
	}
}
