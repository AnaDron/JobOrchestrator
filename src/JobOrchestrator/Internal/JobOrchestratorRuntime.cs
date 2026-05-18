using System.Collections;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// <para>
/// Surface — handle-API: <see cref="this[string]"/> возвращает cached <see cref="StageHandle"/>,
/// далее indexer-композиция → <see cref="InstanceHandle"/>. Все mutation-операции (Trigger/Register*/WaitFor*)
/// делегируют в internal-методы этого класса.
/// </para>
/// </summary>
internal sealed class JobOrchestratorRuntime : IJobOrchestrator {
	private readonly Channel<OrchestratorEvent> _channel;
	private readonly InstanceManager _instances;
	private readonly OrchestratorLifecycle _lifecycle;
	private readonly SuccessWaiters _successWaiters;
	private readonly OutcomeWaiters _outcomeWaiters;
	private readonly ILogger<JobOrchestratorRuntime> _logger;
	private readonly Dictionary<string, StageHandle> _stageHandles;
	private readonly HashSet<string> _knownDomains;
	private readonly ConcurrentDictionary<string, DomainScopedJobOrchestrator> _domainHandles = new(StringComparer.Ordinal);

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
		_instances = instances;
		_lifecycle = lifecycle;
		_successWaiters = successWaiters;
		_outcomeWaiters = outcomeWaiters;
		_logger = logger;

		_stageHandles = registry.AllStages.ToDictionary(x => x.Name, x => new StageHandle(this, x), StringComparer.Ordinal);

		// Pre-compute набор известных доменов — извлекаем префикс до DomainSeparator из имени каждой
		// стадии. Стадия без префикса (например, "global") — в _knownDomains не попадает, что корректно:
		// у неё нет домена, через WithDomain она недоступна.
		_knownDomains = new HashSet<string>(StringComparer.Ordinal);
		foreach (var stage in registry.AllStages) {
			var sep = stage.Name.IndexOf(JobOrchestratorBuilder.DomainSeparator);
			if (sep > 0) _knownDomains.Add(stage.Name[..sep]);
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

	/// <summary>
	/// Итерация всех зарегистрированных стадий через cached <see cref="StageHandle"/>-ы. Аллокация —
	/// один <see cref="Dictionary{TKey,TValue}.ValueCollection"/>-enumerator (boxed через интерфейс),
	/// сами handle-объекты переиспользуются.
	/// </summary>
	public IEnumerator<IStageHandle> GetEnumerator() => _stageHandles.Values.GetEnumerator();
	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	public InstancesOverview GetOverview() => _instances.Snapshot();

	/// <summary>
	/// Domain-проекция: <c>orchestrator.WithDomain("evotor")["shops"]</c> эквивалентно
	/// <c>orchestrator["evotor:shops"]</c>. Возвращаемый объект кэшируется per-domain через
	/// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey, Func{TKey,TValue})"/>:
	/// повторные вызовы с тем же <paramref name="domain"/> дают тот же объект.
	/// </summary>
	public IDomainScopedJobOrchestrator WithDomain(string domain) {
		ArgumentException.ThrowIfNullOrEmpty(domain);
		if (!_knownDomains.Contains(domain)) {
			throw new ArgumentException(
				$"Домен '{domain}' не зарегистрирован — нет ни одной стадии с префиксом '{domain}{JobOrchestratorBuilder.DomainSeparator}'.",
				nameof(domain));
		}
		return _domainHandles.GetOrAdd(domain, d => new DomainScopedJobOrchestrator(this, d));
	}

	#region Internal API — вызывается StageHandle / InstanceHandle

	/// <summary>
	/// Identity-based trigger: identity уже валидирован в StageHandle-indexer (проверка key-names
	/// против ExpectedKeyNames), поэтому валидация ключей здесь не повторяется.
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

	/// <summary>
	/// Внешний bootstrap keyspace для keyless-эмитера. Работает только для стадий без
	/// <c>DependsOnInstance</c>-зависимостей. Вызывается из <see cref="StageHandle.RegisterKey"/>.
	/// </summary>
	internal void RegisterKey(StageDescriptor stage, string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stage);
		Log.RegisterKeyExternal(_logger, stage.Name, key, null);
		_channel.Writer.Publish(new KeyAddedEvent(source, key));
	}

	internal void UnregisterKey(StageDescriptor stage, string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stage);
		Log.UnregisterKeyExternal(_logger, stage.Name, key, null);
		_channel.Writer.Publish(new KeyRemovedEvent(source, key));
	}

	#endregion

	/// <summary>
	/// Резолвит keyless-инстанс стадии для внешнего RegisterKey/UnregisterKey. Бросает
	/// <see cref="InvalidOperationException"/> для стадий с <c>DependsOnInstance</c>-зависимостями
	/// (там нет ambiguity-free Source) и для стадий, чей keyless-инстанс ещё не создан bootstrap-ом.
	/// </summary>
	private Instance ResolveKeylessSource(StageDescriptor stage) {
		if (stage.ExpectedKeyNames.Count != 0) {
			throw new InvalidOperationException(
				$"Стадия '{stage.Name}' имеет ключевые зависимости. Внешний RegisterKey/UnregisterKey работает только для keyless-эмитеров; используйте JobContext.AddKeyAsync/RemoveKeyAsync из ExecuteAsync.");
		}
		var source = _instances.Find(new InstanceIdentity(stage));
		if (source is null) {
			throw new InvalidOperationException(
				$"Keyless-инстанс для стадии '{stage.Name}' ещё не создан (оркестратор не стартован?).");
		}
		return source;
	}

	private void ThrowIfFaulted() {
		if (_lifecycle.IsFaulted) {
			throw new InvalidOperationException("Оркестратор находится в Faulted-состоянии — операции недоступны до рестарта.");
		}
	}

	/// <summary>
	/// Pre-allocated LoggerMessage-делегаты для внешнего API. EventId-ы 6xxx — диапазон Runtime.
	/// Operational visibility: видно кто и когда дёргал manual-triggers.
	/// </summary>
	private static class Log {
		public static readonly Action<ILogger, string, TriggerResult, Exception?> TriggerCompleted =
			LoggerMessage.Define<string, TriggerResult>(LogLevel.Debug, new EventId(6001, nameof(TriggerCompleted)),
				"TriggerAsync({StageName}) → {Result}");

		public static readonly Action<ILogger, string, Exception?> TriggerFaulted =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(6002, nameof(TriggerFaulted)),
				"TriggerAsync({StageName}) → Faulted (оркестратор крашнулся)");

		public static readonly Action<ILogger, string, string, Exception?> RegisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6005, nameof(RegisterKeyExternal)),
				"RegisterKey(stage={StageName}, key={Key}) внешний вызов (bootstrap)");

		public static readonly Action<ILogger, string, string, Exception?> UnregisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6006, nameof(UnregisterKeyExternal)),
				"UnregisterKey(stage={StageName}, key={Key}) внешний вызов");
	}
}
