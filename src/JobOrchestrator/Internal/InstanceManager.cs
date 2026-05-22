using System.Collections.Concurrent;
using System.Collections.ObjectModel;

namespace JobOrchestrator.Internal;

/// <summary>
/// Owner runtime-состояния <see cref="Instance"/>-ов. Lookup идёт по <see cref="InstanceIdentity"/>
/// (canonical hash-equality по <c>(StageName, EncodedKey)</c>). Структуры на <see cref="ConcurrentDictionary{TKey,TValue}"/>:
/// writes — из event-loop-consumer-потока (single writer); reads — из любого потока
/// (<see cref="DueScanner"/>, <see cref="IJobOrchestrator.GetOverview"/>), без блокировок.
/// <para>
/// <b>Life-cycle hooks</b> (<see cref="InstanceAdded"/>/<see cref="InstanceRemoved"/>) — invoked после
/// успешного <see cref="Add"/>/<see cref="Remove"/>. Источник для <see cref="IStageHandle.Changes"/>:
/// <see cref="JobOrchestratorRuntime"/> подписывается на эти callback'и и dispatch'ит в соответствующий
/// <see cref="StageHandle"/>. Callback'и вызываются из event-loop-consumer-потока (single-writer),
/// поэтому делегаты должны быть быстрыми и не блокирующими.
/// </para>
/// </summary>
internal sealed class InstanceManager {
	private readonly ConcurrentDictionary<InstanceIdentity, Instance> _instances = new();
	private readonly ConcurrentDictionary<StageDescriptor, ConcurrentDictionary<Instance, byte>> _byStage = new();

	/// <summary>Hook на successful <see cref="Add"/>.</summary>
	public Action<Instance>? InstanceAdded { get; set; }

	/// <summary>Hook на successful <see cref="Remove"/>.</summary>
	public Action<Instance>? InstanceRemoved { get; set; }

	public bool Exists(InstanceIdentity identity) => _instances.ContainsKey(identity);

	public Instance? Find(InstanceIdentity identity) =>
		_instances.TryGetValue(identity, out var inst) ? inst : null;

	public void Add(Instance instance) {
		if (!_instances.TryAdd(instance.Identity, instance)) {
			throw new InvalidOperationException($"Дубль инстанса в InstanceManager: {instance.Identity.FullyQualifiedName}");
		}
		var set = _byStage.GetOrAdd(instance.Stage, _ => new ConcurrentDictionary<Instance, byte>());
		set.TryAdd(instance, 0);
		InstanceAdded?.Invoke(instance);
	}

	public bool Remove(Instance instance) {
		var removed = _instances.TryRemove(instance.Identity, out _);
		if (removed) {
			if (_byStage.TryGetValue(instance.Stage, out var set)) {
				set.TryRemove(instance, out _);
			}
			InstanceRemoved?.Invoke(instance);
		}
		return removed;
	}

	/// <summary>O(1)-доступ к инстансам стадии через secondary index. Thread-safe.</summary>
	public IReadOnlyCollection<Instance> InstancesOf(StageDescriptor stage) =>
		_byStage.TryGetValue(stage, out var set) ? (IReadOnlyCollection<Instance>)set.Keys : ReadOnlyCollection<Instance>.Empty;

	/// <summary>Все инстансы. Snapshot enumeration — безопасно итерировать одновременно с мутациями.</summary>
	public ICollection<Instance> All => _instances.Values;

	public int Count => _instances.Count;

	/// <summary>
	/// Снимок состояния всех инстансов. Lock-free через atomic-reads Instance-полей.
	/// Snapshot полей внутри одного инстанса согласован (через <see cref="JobMetrics"/>-record), но
	/// между разными инстансами snapshot eventually-consistent. Это приемлемо для диагностики.
	/// </summary>
	public InstancesOverview Snapshot() {
		var infos = new List<InstanceInfo>(_instances.Count);
		foreach (var i in _instances.Values) infos.Add(i.ToInstanceInfo());
		return new InstancesOverview(infos);
	}
}
