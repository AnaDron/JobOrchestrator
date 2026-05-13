using System.Threading.Channels;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// <list type="bullet">
/// <item><c>TriggerAsync</c>: валидация ключей → публикация <see cref="ManualTriggerRequestedEvent"/> в Channel, await TCS.</item>
/// <item><c>Register/UnregisterKey</c>: проверка существования стадии → публикация <see cref="KeyAddedEvent"/>/<see cref="KeyRemovedEvent"/>.</item>
/// <item><c>GetOverview</c>: синхронный snapshot через atomic-reads <see cref="StageInstance"/>-полей. Lock-free, без RPC.</item>
/// </list>
/// </summary>
internal sealed class JobOrchestratorRuntime(
	Channel<OrchestratorEvent> channel,
	StageRegistry registry,
	InstanceManager instances,
	OrchestratorLifecycle lifecycle
) : IJobOrchestrator {
	public bool IsFaulted => lifecycle.IsFaulted;

	public async Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (lifecycle.IsFaulted) return TriggerResult.Faulted;
		if (!registry.TryGet(stageName, out _)) return TriggerResult.NotFound;

		var keys = dependencyKeys ?? new Dictionary<string, string>(StringComparer.Ordinal);
		if (!ValidateKeys(registry.ExpectedKeyNames(stageName), keys)) return TriggerResult.InvalidKeys;

		var tcs = new TaskCompletionSource<TriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(stageName, keys, tcs);
		try {
			await channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			return TriggerResult.Faulted;
		}
		return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
	}

	public void RegisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		channel.Writer.Publish(new KeyAddedEvent(source, key));
	}

	public void UnregisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		channel.Writer.Publish(new KeyRemovedEvent(source, key));
	}

	/// <summary>
	/// Резолвит keyless-инстанс стадии для внешнего <see cref="RegisterKey"/>/<see cref="UnregisterKey"/>.
	/// Работает только для стадий без <c>DependsOnInstance</c>-зависимостей (там нет ambiguity, какой
	/// инстанс-эмитер использовать как Source). Для ключевых стадий — InvalidOperationException
	/// (BL должна использовать <see cref="JobContext.AddKey"/> изнутри сервиса).
	/// </summary>
	private StageInstance ResolveKeylessSource(string stageName) {
		if (!registry.TryGet(stageName, out _)) {
			throw new ArgumentException($"Стадия '{stageName}' не зарегистрирована в графе.", nameof(stageName));
		}
		if (registry.ExpectedKeyNames(stageName).Count != 0) {
			throw new InvalidOperationException(
				$"Стадия '{stageName}' имеет ключевые зависимости. Внешний RegisterKey/UnregisterKey работает только для keyless-эмитеров; используйте JobContext.AddKey/RemoveKey из ExecuteAsync.");
		}
		var empty = new Dictionary<string, string>(StringComparer.Ordinal);
		var source = instances.Find(stageName, empty);
		if (source is null) {
			throw new InvalidOperationException(
				$"Keyless-инстанс для стадии '{stageName}' ещё не создан (оркестратор не стартован?).");
		}
		return source;
	}

	public InstancesOverview GetOverview() {
		ThrowIfFaulted();
		return instances.Snapshot();
	}

	/// <summary>
	/// Проверяет, что набор имён ключей соответствует ожидаемым именам, включая транзитивно унаследованные
	/// через цепочку <c>DependsOn</c>-родителей (см. <see cref="StageRegistry.ExpectedKeyNames"/>).
	/// Точное соответствие: те же имена в одинаковом наборе. Для безключевой стадии — пустой словарь.
	/// </summary>
	private static bool ValidateKeys(IReadOnlyList<string> expected, IReadOnlyDictionary<string, string> keys) {
		if (keys.Count != expected.Count) return false;
		foreach (var name in expected) {
			if (!keys.ContainsKey(name)) return false;
		}
		return true;
	}

	private void ThrowIfFaulted() {
		if (lifecycle.IsFaulted) {
			throw new InvalidOperationException("Оркестратор находится в Faulted-состоянии — операции недоступны до рестарта.");
		}
	}
}
