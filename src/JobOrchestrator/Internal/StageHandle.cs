using System.Collections.Immutable;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IStageHandle"/>: cached один раз на стадию в <see cref="JobOrchestratorRuntime"/>.
/// Все методы — thin proxy к Runtime, делегирующий по <see cref="StageDescriptor"/>.
/// <para>
/// <c>record class</c> — для value-equality по <c>(Runtime, Stage)</c>. На практике все handles
/// одной стадии в одном orchestrator-instance reference-equal (кеш), но value-семантика обеспечивает
/// корректное поведение, если каким-то образом окажется два экземпляра.
/// </para>
/// </summary>
internal sealed record class StageHandle(JobOrchestratorRuntime Runtime, StageDescriptor Stage) : IStageHandle {
	public string Name => Stage.Name;

	public IInstanceHandle this[InstanceKey marker] {
		// marker — typed sentinel, значение игнорируется (есть только одно: InstanceKey.None).
		// Validation: keyless-индексатор валиден только если у стадии нет ожидаемых ключей.
		get {
			ValidateKeyless();
			return new InstanceHandle(Runtime, new InstanceIdentity(Stage));
		}
	}

	public IInstanceHandle this[(string Name, string Value) key] {
		get {
			var identity = new InstanceIdentity(Stage, ValidateAndBuild([key]));
			return new InstanceHandle(Runtime, identity);
		}
	}

	public IInstanceHandle this[(string Name, string Value) key1, (string Name, string Value) key2] {
		get {
			var identity = new InstanceIdentity(Stage, ValidateAndBuild([key1, key2]));
			return new InstanceHandle(Runtime, identity);
		}
	}

	public IInstanceHandle this[params ReadOnlySpan<(string Name, string Value)> keys] {
		get {
			if (keys.IsEmpty) {
				ValidateKeyless();
				return new InstanceHandle(Runtime, new InstanceIdentity(Stage));
			}
			var identity = new InstanceIdentity(Stage, ValidateAndBuild(keys));
			return new InstanceHandle(Runtime, identity);
		}
	}

	public void RegisterKey(string key) => Runtime.RegisterKey(Stage.Name, key);
	public void UnregisterKey(string key) => Runtime.UnregisterKey(Stage.Name, key);

	public IReadOnlyList<IInstanceHandle> AllInstances {
		get {
			var instances = Runtime.InstancesOf(Stage);
			var list = new List<IInstanceHandle>(instances.Count);
			foreach (var instance in instances) {
				// Reuse instance.Identity — без реконструкции (pre-sort/EncodedKey/FQN уже вычислены).
				list.Add(new InstanceHandle(Runtime, instance.Identity));
			}
			return list;
		}
	}

	private void ValidateKeyless() {
		if (Stage.ExpectedKeyNames.Count != 0) {
			throw new ArgumentException(
				$"Стадия '{Stage.Name}' ожидает {Stage.ExpectedKeyNames.Count} key-component(s) " +
				$"[{string.Join(", ", Stage.ExpectedKeyNames)}]; для keyless-доступа использовать только у стадий без зависимостей-ключей.",
				nameof(InstanceKey));
		}
	}

	/// <summary>
	/// Валидирует, что переданные key-names ровно соответствуют <see cref="StageDescriptor.ExpectedKeyNames"/>
	/// (без duplicates, без неизвестных, без missing). Бросает <see cref="ArgumentException"/> при mismatch —
	/// fail-fast на handle-construction, чтобы юзер увидел проблему сразу, а не получил silent-NotFound
	/// в TriggerAsync.
	/// </summary>
	private ImmutableDictionary<string, string> ValidateAndBuild(ReadOnlySpan<(string Name, string Value)> keys) {
		var expected = Stage.ExpectedKeyNames;
		if (keys.Length != expected.Count) {
			throw new ArgumentException(
				$"Стадия '{Stage.Name}' ожидает {expected.Count} key-component(s) [{string.Join(", ", expected)}], передано {keys.Length}.",
				nameof(keys));
		}
		var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.Ordinal);
		foreach (var (name, value) in keys) {
			if (!ContainsExpected(expected, name)) {
				throw new ArgumentException(
					$"Стадия '{Stage.Name}': key-name '{name}' не входит в ExpectedKeyNames [{string.Join(", ", expected)}].",
					nameof(keys));
			}
			if (!builder.TryAdd(name, value)) {
				throw new ArgumentException(
					$"Стадия '{Stage.Name}': дублирующийся key-name '{name}'.",
					nameof(keys));
			}
		}
		return builder.ToImmutable();
	}

	private static bool ContainsExpected(IReadOnlyList<string> expected, string name) {
		foreach (var e in expected) {
			if (string.Equals(e, name, StringComparison.Ordinal)) return true;
		}
		return false;
	}
}
