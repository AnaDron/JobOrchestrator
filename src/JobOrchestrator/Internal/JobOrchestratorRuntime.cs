using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// <para>
/// Новый handle-API (<see cref="IJobOrchestrator.this[string]"/> → <see cref="IStageHandle"/> →
/// <see cref="IInstanceHandle"/>) — primary surface; flat-методы (TriggerAsync(stageName, …) и т.п.)
/// сохранены как <c>[Obsolete]</c> для backward-compatibility и будут удалены в следующей мажорной.
/// </para>
/// <list type="bullet">
/// <item><c>TriggerAsync(Identity)</c>: публикация <see cref="ManualTriggerRequestedEvent"/> в Channel, await TCS.</item>
/// <item><c>Register/UnregisterKey(stageName)</c>: проверка существования стадии → публикация <see cref="KeyAddedEvent"/>/<see cref="KeyRemovedEvent"/>.</item>
/// <item><c>GetOverview</c>: синхронный snapshot через atomic-reads <see cref="Instance"/>-полей. Lock-free, без RPC. Доступен и в Faulted (для диагностики).</item>
/// </list>
/// </summary>
internal sealed class JobOrchestratorRuntime : IJobOrchestrator {
	private readonly Channel<OrchestratorEvent> _channel;
	private readonly StageRegistry _registry;
	private readonly InstanceManager _instances;
	private readonly OrchestratorLifecycle _lifecycle;
	private readonly SuccessWaiters _successWaiters;
	private readonly OutcomeWaiters _outcomeWaiters;
	private readonly ILogger<JobOrchestratorRuntime> _logger;
	private readonly Dictionary<string, StageHandle> _stageHandles;

	public JobOrchestratorRuntime(
		Channel<OrchestratorEvent> channel,
		StageRegistry registry,
		InstanceManager instances,
		OrchestratorLifecycle lifecycle,
		SuccessWaiters successWaiters,
		OutcomeWaiters outcomeWaiters,
		ILogger<JobOrchestratorRuntime> logger
	) {
		_channel = channel;
		_registry = registry;
		_instances = instances;
		_lifecycle = lifecycle;
		_successWaiters = successWaiters;
		_outcomeWaiters = outcomeWaiters;
		_logger = logger;
		// One StageHandle per stage, immutable for process lifetime — populated once at construction.
		_stageHandles = new Dictionary<string, StageHandle>(registry.AllStages.Count, StringComparer.Ordinal);
		foreach (var stage in registry.AllStages) {
			_stageHandles[stage.Name] = new StageHandle(this, stage);
		}
	}

	public bool IsFaulted => _lifecycle.IsFaulted;

	/// <summary>
	/// Root indexer для handle-API: <c>orchestrator["stageName"]</c> → <see cref="IStageHandle"/>.
	/// O(1) hash-lookup, cached handle — нулевая allocation.
	/// </summary>
	public IStageHandle this[string stageName] {
		get {
			ArgumentException.ThrowIfNullOrEmpty(stageName);
			if (!_stageHandles.TryGetValue(stageName, out var handle)) {
				throw new ArgumentException($"Стадия '{stageName}' не зарегистрирована в графе.", nameof(stageName));
			}
			return handle;
		}
	}

	public InstancesOverview GetOverview() => _instances.Snapshot();

	#region Identity-based primary API (используется handle-API)

	/// <summary>
	/// Identity-based trigger: identity уже валидирован при создании handle (через <see cref="StageRegistry"/>),
	/// поэтому валидация ключей не повторяется. Семантика идентична flat-варианту.
	/// </summary>
	internal async Task<TriggerResult> TriggerAsync(InstanceIdentity identity, CancellationToken ct = default) {
		if (_lifecycle.IsFaulted) {
			Log.TriggerFaulted(_logger, identity.Stage.Name, null);
			return TriggerResult.Faulted;
		}
		var tcs = new TaskCompletionSource<TriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(identity, tcs);
		try {
			await _channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			return TriggerResult.Faulted;
		}
		var result = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
		Log.TriggerCompleted(_logger, identity.Stage.Name, result, null);
		return result;
	}

	internal Task WaitForStageSuccessAsync(InstanceIdentity identity, CancellationToken ct = default) {
		var task = _successWaiters.Register(identity, ct);

		// Fast-path: если инстанс УЖЕ существует И УЖЕ имел success — сразу резолвим.
		var existing = _instances.Find(identity);
		if (existing is not null
			&& existing.State != InstanceLifecycleState.Terminating
			&& existing.Metrics.LastSuccess is not null) {
			_successWaiters.SignalSuccess(identity);
		}

		return task;
	}

	internal Task<StageOutcome> WaitForStageOutcomeAsync(InstanceIdentity identity, CancellationToken ct = default) =>
		_outcomeWaiters.Register(identity, ct);

	internal Instance? FindInstance(InstanceIdentity identity) => _instances.Find(identity);

	internal ICollection<Instance> InstancesOf(StageDescriptor stage) => _instances.InstancesOf(stage);

	#endregion

	#region Public flat-API (handle-API — primary; flat — [Obsolete] для transition)

	[Obsolete("Используйте orchestrator[stageName][keys].TriggerAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB001")]
	public async Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (_lifecycle.IsFaulted) {
			Log.TriggerFaulted(_logger, stageName, null);
			return TriggerResult.Faulted;
		}
		if (!_registry.TryGet(stageName, out var stage)) {
			Log.TriggerNotFound(_logger, stageName, null);
			return TriggerResult.NotFound;
		}
		if (!ValidateKeys(stage!.ExpectedKeyNames, dependencyKeys)) {
			Log.TriggerInvalidKeys(_logger, stageName, null);
			return TriggerResult.InvalidKeys;
		}
		return await TriggerAsync(new InstanceIdentity(stage, dependencyKeys), ct).ConfigureAwait(false);
	}

	public void RegisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		Log.RegisterKeyExternal(_logger, stageName, key, null);
		_channel.Writer.Publish(new KeyAddedEvent(source, key));
	}

	public void UnregisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		Log.UnregisterKeyExternal(_logger, stageName, key, null);
		_channel.Writer.Publish(new KeyRemovedEvent(source, key));
	}

	[Obsolete("Используйте orchestrator[stageName][keys].WaitForSuccessAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB002")]
	public Task WaitForStageSuccessAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) => WaitForStageSuccessAsync(ResolveIdentity(stageName, dependencyKeys), ct);

	[Obsolete("Используйте orchestrator[stageName][keys].WaitForOutcomeAsync(). Будет удалён в следующей мажорной версии.", DiagnosticId = "JOB003")]
	public Task<StageOutcome> WaitForStageOutcomeAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) => WaitForStageOutcomeAsync(ResolveIdentity(stageName, dependencyKeys), ct);

	#endregion

	/// <summary>
	/// Резолвит keyless-инстанс стадии для внешнего <see cref="RegisterKey"/>/<see cref="UnregisterKey"/>.
	/// Работает только для стадий без <c>DependsOnInstance</c>-зависимостей (там нет ambiguity, какой
	/// инстанс-эмитер использовать как Source). Для ключевых стадий — InvalidOperationException
	/// (BL должна использовать <see cref="JobContext.AddKey"/> изнутри сервиса).
	/// </summary>
	private Instance ResolveKeylessSource(string stageName) {
		if (!_registry.TryGet(stageName, out var stage)) {
			throw new ArgumentException($"Стадия '{stageName}' не зарегистрирована в графе.", nameof(stageName));
		}
		if (stage!.ExpectedKeyNames.Count != 0) {
			throw new InvalidOperationException(
				$"Стадия '{stageName}' имеет ключевые зависимости. Внешний RegisterKey/UnregisterKey работает только для keyless-эмитеров; используйте JobContext.AddKey/RemoveKey из ExecuteAsync.");
		}
		var source = _instances.Find(new InstanceIdentity(stage));
		if (source is null) {
			throw new InvalidOperationException(
				$"Keyless-инстанс для стадии '{stageName}' ещё не создан (оркестратор не стартован?).");
		}
		return source;
	}

	/// <summary>
	/// Резолвит <see cref="InstanceIdentity"/> с валидацией: stage существует в графе, набор ключей
	/// соответствует <see cref="StageDescriptor.ExpectedKeyNames"/>. Используется flat-обёртками
	/// WaitFor-методов.
	/// </summary>
	private InstanceIdentity ResolveIdentity(string stageName, IReadOnlyDictionary<string, string>? dependencyKeys) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (!_registry.TryGet(stageName, out var stage)) {
			throw new ArgumentException($"Стадия '{stageName}' не зарегистрирована в графе.", nameof(stageName));
		}
		if (!ValidateKeys(stage!.ExpectedKeyNames, dependencyKeys)) {
			throw new ArgumentException(
				$"Набор ключей для стадии '{stageName}' не соответствует ExpectedKeyNames.",
				nameof(dependencyKeys));
		}
		return new InstanceIdentity(stage, dependencyKeys);
	}

	/// <summary>
	/// Проверяет, что набор имён ключей соответствует ожидаемым именам, включая транзитивно унаследованные
	/// через цепочку <c>DependsOn</c>-родителей (см. <see cref="StageDescriptor.ExpectedKeyNames"/>).
	/// <paramref name="keys"/> = <c>null</c> трактуется как пустой словарь — валидно только для keyless-стадий.
	/// </summary>
	private static bool ValidateKeys(IReadOnlyList<string> expected, IReadOnlyDictionary<string, string>? keys) {
		if (keys is null) return expected.Count == 0;
		if (keys.Count != expected.Count) return false;
		foreach (var name in expected) {
			if (!keys.ContainsKey(name)) return false;
		}
		return true;
	}

	private void ThrowIfFaulted() {
		if (_lifecycle.IsFaulted) {
			throw new InvalidOperationException("Оркестратор находится в Faulted-состоянии — операции недоступны до рестарта.");
		}
	}

	/// <summary>
	/// Pre-allocated LoggerMessage-делегаты для внешнего API (TriggerAsync/RegisterKey/UnregisterKey).
	/// EventId-ы 6xxx — диапазон Runtime. Operational visibility: видно кто и когда дёргал manual-triggers.
	/// </summary>
	private static class Log {
		public static readonly Action<ILogger, string, TriggerResult, Exception?> TriggerCompleted =
			LoggerMessage.Define<string, TriggerResult>(LogLevel.Debug, new EventId(6001, nameof(TriggerCompleted)),
				"TriggerAsync({StageName}) → {Result}");

		public static readonly Action<ILogger, string, Exception?> TriggerFaulted =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(6002, nameof(TriggerFaulted)),
				"TriggerAsync({StageName}) → Faulted (оркестратор крашнулся)");

		public static readonly Action<ILogger, string, Exception?> TriggerNotFound =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(6003, nameof(TriggerNotFound)),
				"TriggerAsync({StageName}) → NotFound (стадия не зарегистрирована или инстанс не существует)");

		public static readonly Action<ILogger, string, Exception?> TriggerInvalidKeys =
			LoggerMessage.Define<string>(LogLevel.Warning, new EventId(6004, nameof(TriggerInvalidKeys)),
				"TriggerAsync({StageName}) → InvalidKeys (набор ключей не соответствует ExpectedKeyNames стадии)");

		public static readonly Action<ILogger, string, string, Exception?> RegisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6005, nameof(RegisterKeyExternal)),
				"RegisterKey(stage={StageName}, key={Key}) внешний вызов (bootstrap)");

		public static readonly Action<ILogger, string, string, Exception?> UnregisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6006, nameof(UnregisterKeyExternal)),
				"UnregisterKey(stage={StageName}, key={Key}) внешний вызов");
	}
}
