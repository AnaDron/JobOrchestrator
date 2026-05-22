using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IStageHandle"/>. Cached один раз на стадию в <see cref="JobOrchestratorRuntime"/>.
/// <para>
/// Owner материализованных <see cref="InstanceHandle"/>-ов своей стадии: при <see cref="NotifyAdded"/>
/// handle создаётся один раз и cached в <c>_instanceHandles</c>; при <see cref="NotifyRemoved"/> — снимается.
/// Iteration / <see cref="Changes"/>-replay / index-lookup используют тот же cached reference, без
/// повторных аллокаций и без обращений к <see cref="JobOrchestratorRuntime.InstancesOf"/>.
/// </para>
/// <para>
/// <b>Race-free Changes-replay через tagged generation:</b> каждый cached handle несёт <c>AddedGen</c> —
/// generation на момент материализации. Replay фильтрует <c>AddedGen ≤ snapshotGen</c>, live-loop
/// фильтрует <c>gen &gt; snapshotGen</c> — два фильтра пересекаются в пустоту, дубликатов нет.
/// </para>
/// </summary>
internal sealed class StageHandle(JobOrchestratorRuntime runtime, StageDescriptor stage, IDomainHandle domain) : IStageHandle {
	private const int ChangesBufferCapacity = 256;

	public IDomainHandle Domain => domain;
	public string Name => stage.Name;

	private long _generation;

	// CHM-as-Set: subscriber add/remove из caller-потоков concurrent с notify-iteration из event-loop —
	// нужен lock-free thread-safe set. В BCL ConcurrentHashSet<T> отсутствует, value=byte неиспользуется.
	private readonly ConcurrentDictionary<BoundedSubscriber<(StageChange Change, long Generation)>, byte> _subscribers = new();

	// Cached handles материализованных инстансов: identity-preserved между replay и live-notifications.
	// AddedGen — generation на момент материализации; используется в Changes-replay для фильтрации
	// (см. IterateChangesAsync). Single-writer записи (NotifyAdded/Removed из event-loop-consumer),
	// concurrent lookup'ы из любого caller-потока — CHM обеспечивает thread-safety.
	private readonly ConcurrentDictionary<InstanceIdentity, (InstanceHandle Handle, long AddedGen)> _instanceHandles = new();

	public int Count => _instanceHandles.Count;

	public IInstanceHandle this[InstanceKeys keys] {
		get {
			var identity = BuildIdentity(keys);
			// Материализован — cached handle (reference-stable между call'ами и notifications).
			// Не материализован / удалён — on-demand identity-handle (без кэша, чтобы не плодить мёртвые
			// ссылки от случайных lookup'ов).
			return _instanceHandles.TryGetValue(identity, out var entry) ? entry.Handle : ToHandle(identity);
		}
	}

	public void RegisterKey(string key) => runtime.RegisterKey(stage, key);
	public void UnregisterKey(string key) => runtime.UnregisterKey(stage, key);

	public IEnumerator<IInstanceHandle> GetEnumerator() {
		foreach (var entry in _instanceHandles.Values) yield return entry.Handle;
	}

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	public IAsyncEnumerable<StageChange> Changes => IterateChangesAsync();

	private async IAsyncEnumerable<StageChange> IterateChangesAsync([EnumeratorCancellation] CancellationToken ct = default) {
		// Регистрируем subscriber'а ДО snapshot generation: любое Added/Removed между этими шагами
		// окажется в канале и будет отфильтровано по generation > snapshotGen.
		await using var subscriber = new BoundedSubscriber<(StageChange Change, long Generation)>(ChangesBufferCapacity, _subscribers);
		_subscribers.TryAdd(subscriber, 0);
		var snapshotGen = Interlocked.Read(ref _generation);
		// Replay: yield Added для handle'ов с AddedGen <= snapshotGen. Записи с AddedGen > snapshotGen —
		// race-window concurrent Add; они придут через live-loop.
		foreach (var entry in _instanceHandles.Values) {
			if (entry.AddedGen <= snapshotGen) yield return new StageChange(entry.Handle, StageChangeKind.Added);
		}
		// Live-loop: фильтруем события generation <= snapshotGen (они уже в replay).
		await foreach (var (change, gen) in subscriber.Reader.ReadAllAsync(ct).ConfigureAwait(false)) {
			if (gen > snapshotGen) yield return change;
		}
	}

	internal void NotifyAdded(Instance instance) {
		// Single-writer (event-loop) — writer-vs-writer concurrency нет; race есть writer-vs-reader
		// (subscriber-поток читает _generation + cache + channel).
		//
		// ПОРЯДОК cache → gen ↑ → publish обеспечивает инвариант: «subscriber, читающий newGen, гарантированно
		// видит handle в cache через replay». Volatile.Write — release-barrier (важно для ARM/weak-memory):
		// cache-write happens-before gen-write для читателя через Volatile.Read.
		//
		// Альтернатива cache → publish → gen ↑ давала бы lost-event для subscriber'а, регистрирующегося
		// между publish-foreach (без него) и gen ↑ (snapshotGen ещё старый, replay скипает handle с
		// AddedGen=newGen > snapshotGen, live-канал у него пустой).
		var handle = ToHandle(instance);
		var nextGen = Volatile.Read(ref _generation) + 1;
		_instanceHandles[instance.Identity] = (handle, nextGen);
		Volatile.Write(ref _generation, nextGen);
		var change = new StageChange(handle, StageChangeKind.Added);
		foreach (var sub in _subscribers.Keys) sub.Publish((change, nextGen));
	}

	internal void NotifyRemoved(Instance instance) {
		// Симметричный порядок: cache mutation → gen ↑ → publish (см. NotifyAdded для обоснования).
		// Fallback к on-demand handle — если NotifyAdded ещё не отработал (теоретически невозможно при
		// single-writer event-loop, но не глотаем silently).
		var handle = _instanceHandles.TryRemove(instance.Identity, out var entry) ? entry.Handle : ToHandle(instance);
		var nextGen = Volatile.Read(ref _generation) + 1;
		Volatile.Write(ref _generation, nextGen);
		var change = new StageChange(handle, StageChangeKind.Removed);
		foreach (var sub in _subscribers.Keys) sub.Publish((change, nextGen));
	}

	internal void CompleteAllSubscribers() {
		foreach (var sub in _subscribers.Keys) sub.Complete();
		_subscribers.Clear();
	}

	/// <summary>Фабрика identity-based handle поверх локального <c>runtime</c>-захвата.</summary>
	private InstanceHandle ToHandle(Instance instance) => new(runtime, instance.Identity);
	private InstanceHandle ToHandle(InstanceIdentity identity) => new(runtime, identity);

	private InstanceIdentity BuildIdentity(InstanceKeys keys) {
		var expected = stage.ExpectedKeyNames;
		if (keys.Count != expected.Count) {
			throw new ArgumentException(
				$"Стадия '{stage.Name}' ожидает {expected.Count} key-component(s) [{string.Join(", ", expected)}], передано {keys.Count}.",
				nameof(keys));
		}
		if (keys.Count == 0) return new InstanceIdentity(stage);
		var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.Ordinal);
		foreach (var kv in keys) {
			if (!ContainsExpected(expected, kv.Key)) {
				throw new ArgumentException(
					$"Стадия '{stage.Name}': key-name '{kv.Key}' не входит в ExpectedKeyNames [{string.Join(", ", expected)}].",
					nameof(keys));
			}
			builder.Add(kv.Key, kv.Value);
		}
		return new InstanceIdentity(stage, builder.ToImmutable());
	}

	private static bool ContainsExpected(IReadOnlyList<string> expected, string name) {
		foreach (var e in expected) {
			if (string.Equals(e, name, StringComparison.Ordinal)) return true;
		}
		return false;
	}
}
