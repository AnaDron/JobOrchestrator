namespace JobOrchestrator.Internal;

/// <summary>
/// Реестр keyspace для каждой стадии. Все операции — через event-loop consumer thread (single-threaded).
/// </summary>
internal sealed class KeyspaceRegistry {
	private readonly Dictionary<string, HashSet<string>> _byStage = new(StringComparer.Ordinal);

	/// <summary>Регистрирует ключ. Возвращает true если ключ был добавлен (false — уже был, идемпотентно).</summary>
	public bool Add(string stageName, string key) {
		if (!_byStage.TryGetValue(stageName, out var set)) {
			set = new HashSet<string>(StringComparer.Ordinal);
			_byStage[stageName] = set;
		}
		return set.Add(key);
	}

	/// <summary>Удаляет ключ. Возвращает true если ключ был удалён (false — не существовал).</summary>
	public bool Remove(string stageName, string key) {
		return _byStage.TryGetValue(stageName, out var set) && set.Remove(key);
	}

	public bool Contains(string stageName, string key) {
		return _byStage.TryGetValue(stageName, out var set) && set.Contains(key);
	}

	public IReadOnlyCollection<string> Get(string stageName) {
		return _byStage.TryGetValue(stageName, out var set)
			? (IReadOnlyCollection<string>)set
			: Array.Empty<string>();
	}
}
