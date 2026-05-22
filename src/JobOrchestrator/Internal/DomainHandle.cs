using System.Collections;
using System.Collections.Frozen;
using System.Collections.Immutable;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IDomainHandle"/> — frozen on construction коллекция стадий одного домена.
/// <para>
/// Является owner'ом своих <see cref="StageHandle"/>-ов: создаёт их в конструкторе из переданных
/// <see cref="StageDescriptor"/>-ов и передаёт <c>this</c> как <see cref="IStageHandle.Domain"/>.
/// Так <see cref="StageHandle.Domain"/> immutable с момента construction — никаких setter'ов и
/// двухфазной инициализации.
/// </para>
/// </summary>
internal sealed class DomainHandle : IDomainHandle {
	private readonly ImmutableArray<StageHandle> _stages;
	private readonly FrozenDictionary<string, StageHandle> _stagesByLocalName;

	public string Name { get; }

	public DomainHandle(
		string name,
		JobOrchestratorRuntime runtime,
		IEnumerable<(string Local, StageDescriptor Descriptor)> entries
	) {
		Name = name;
		var builder = ImmutableArray.CreateBuilder<StageHandle>();
		var byLocal = new Dictionary<string, StageHandle>(StringComparer.Ordinal);
		foreach (var (local, descriptor) in entries) {
			// Передаём this как Domain — reverse-link immutable с момента construction StageHandle.
			var handle = new StageHandle(runtime, descriptor, this);
			builder.Add(handle);
			byLocal.Add(local, handle);
		}
		_stages = builder.ToImmutable();
		_stagesByLocalName = byLocal.ToFrozenDictionary(StringComparer.Ordinal);
	}

	public int Count => _stages.Length;

	public IStageHandle this[string localStageName] {
		get {
			ArgumentException.ThrowIfNullOrEmpty(localStageName);
			if (!_stagesByLocalName.TryGetValue(localStageName, out var handle)) {
				var domainLabel = Name.Length == 0 ? "<root>" : $"'{Name}'";
				throw new ArgumentException(
					$"Стадия '{localStageName}' не зарегистрирована в домене {domainLabel}.",
					nameof(localStageName));
			}
			return handle;
		}
	}

	public IEnumerator<IStageHandle> GetEnumerator() => ((IEnumerable<StageHandle>)_stages).GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
