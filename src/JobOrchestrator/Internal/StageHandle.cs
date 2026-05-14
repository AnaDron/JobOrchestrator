using System.Collections.Immutable;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IStageHandle"/>: cached один раз на стадию в <see cref="JobOrchestratorRuntime"/>.
/// Все методы — thin proxy к Runtime, делегирующий по <see cref="StageDescriptor"/>.
/// </summary>
internal sealed class StageHandle(JobOrchestratorRuntime runtime, StageDescriptor stage) : IStageHandle {
	public string Name => stage.Name;

	public IInstanceHandle this[InstanceKey marker] {
		// marker — typed sentinel, значение игнорируется (есть только одно: InstanceKey.None).
		get => new InstanceHandle(runtime, stage, ImmutableDictionary<string, string>.Empty);
	}

	public IInstanceHandle this[(string Name, string Value) key] =>
		new InstanceHandle(runtime, stage, OneKey(key));

	public IInstanceHandle this[(string Name, string Value) key1, (string Name, string Value) key2] =>
		new InstanceHandle(runtime, stage, TwoKeys(key1, key2));

	public IInstanceHandle this[params ReadOnlySpan<(string Name, string Value)> keys] =>
		new InstanceHandle(runtime, stage, FromSpan(keys));

	public void RegisterKey(string key) => runtime.RegisterKey(stage.Name, key);
	public void UnregisterKey(string key) => runtime.UnregisterKey(stage.Name, key);

	public IReadOnlyList<IInstanceHandle> AllInstances {
		get {
			var list = new List<IInstanceHandle>();
			foreach (var instance in runtime.InstancesOf(stage)) {
				list.Add(new InstanceHandle(runtime, stage, instance.Identity.DependencyKeys));
			}
			return list;
		}
	}

	private static ImmutableDictionary<string, string> OneKey((string Name, string Value) k) =>
		ImmutableDictionary.CreateRange(StringComparer.Ordinal, [new KeyValuePair<string, string>(k.Name, k.Value)]);

	private static ImmutableDictionary<string, string> TwoKeys(
		(string Name, string Value) k1, (string Name, string Value) k2
	) {
		var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.Ordinal);
		builder[k1.Name] = k1.Value;
		builder[k2.Name] = k2.Value;
		return builder.ToImmutable();
	}

	private static ImmutableDictionary<string, string> FromSpan(ReadOnlySpan<(string Name, string Value)> keys) {
		if (keys.IsEmpty) return ImmutableDictionary<string, string>.Empty;
		var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.Ordinal);
		foreach (var (name, value) in keys) builder[name] = value;
		return builder.ToImmutable();
	}
}
