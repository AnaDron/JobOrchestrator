using System.Collections;
using System.Collections.Immutable;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// <para>
/// Surface — handle-API: <see cref="this[string]"/> возвращает cached <see cref="DomainHandle"/>,
/// далее <see cref="IDomainHandle.this[string]"/> → <see cref="StageHandle"/> → <see cref="InstanceHandle"/>.
/// Все mutation-операции (Trigger/Register*/WaitFor*) делегируют в internal-методы этого класса.
/// </para>
/// <para>
/// На construction: разбираем <see cref="StageRegistry"/> по доменам (split по
/// <see cref="JobOrchestratorBuilder.DomainSeparator"/>); root-домен — под ключом <see cref="DomainName.Root"/>;
/// топология frozen на старте, runtime-добавление доменов не поддерживается.
/// </para>
/// <para>
/// Подписываемся на <see cref="InstanceManager.InstanceAdded"/>/<see cref="InstanceManager.InstanceRemoved"/>
/// для dispatch'а в <see cref="StageHandle.NotifyAdded"/>/<see cref="StageHandle.NotifyRemoved"/>
/// — источник <see cref="IStageHandle.Changes"/>.
/// </para>
/// </summary>
internal sealed class JobOrchestratorRuntime : IJobOrchestrator {
	private readonly Channel<OrchestratorEvent> _channel;
	private readonly InstanceManager _instances;
	private readonly OrchestratorLifecycle _lifecycle;
	private readonly ILogger<JobOrchestratorRuntime> _logger;
	private readonly Dictionary<string, StageHandle> _stageHandles;
	private readonly Dictionary<string, DomainHandle> _domainsByName;
	private readonly ImmutableArray<DomainHandle> _domainsOrdered;
	private readonly DomainHandle _rootDomain;

	public JobOrchestratorRuntime(
		Channel<OrchestratorEvent> channel,
		StageRegistry registry,
		InstanceManager instances,
		OrchestratorLifecycle lifecycle,
		ILogger<JobOrchestratorRuntime> logger
	) {
		_channel = channel;
		_instances = instances;
		_lifecycle = lifecycle;
		_logger = logger;

		var entriesByDomain = registry.AllStages
			.Select(s => {
				var n = s.Name.IndexOf(JobOrchestratorBuilder.DomainSeparator);
				return (
					Domain: n > 0 ? s.Name[..n] : DomainName.Root,
					Local: n > 0 ? s.Name[(n + 1)..] : s.Name,
					Descriptor: s
				);
			}).GroupBy(x => x.Domain, x => (x.Local, x.Descriptor), StringComparer.Ordinal)
			.ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);
		entriesByDomain.TryAdd(DomainName.Root, []);

		_domainsOrdered = [
			..entriesByDomain.Keys
				.Where(d => d != DomainName.Root)
				.Distinct(StringComparer.Ordinal)
				.Prepend(DomainName.Root)
				.Select(d => new DomainHandle(d, this, entriesByDomain[d]))
		];
		_domainsByName = _domainsOrdered.ToDictionary(d => d.Name, StringComparer.Ordinal);
		_rootDomain = _domainsByName[DomainName.Root];
		// Aggregate full-name → handle для O(1) GetStageHandle. DomainHandle сам owns StageHandle'ы
		// и перечисляет их через IEnumerable<IStageHandle>; cast вниз безопасен в этом scope.
		_stageHandles = _domainsOrdered
			.SelectMany(d => d.Cast<StageHandle>())
			.ToDictionary(s => s.Name, StringComparer.Ordinal);

		// Подписка на life-cycle InstanceManager → dispatch в соответствующий StageHandle.
		instances.InstanceAdded = OnInstanceAdded;
		instances.InstanceRemoved = OnInstanceRemoved;
	}

	public bool IsFaulted => _lifecycle.IsFaulted;

	public IDomainHandle Root => _rootDomain;

	public IDomainHandle this[string domainName] {
		get {
			ArgumentNullException.ThrowIfNull(domainName);
			if (!_domainsByName.TryGetValue(domainName, out var handle)) {
				var label = domainName.Length == 0 ? "<root>" : $"'{domainName}'";
				throw new ArgumentException($"Домен {label} не зарегистрирован.", nameof(domainName));
			}
			return handle;
		}
	}

	public int Count => _domainsOrdered.Length;

	public IEnumerator<IDomainHandle> GetEnumerator() => ((IEnumerable<DomainHandle>)_domainsOrdered).GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

	public InstancesOverview GetOverview() => _instances.Snapshot();

	#region Internal API — вызывается StageHandle / InstanceHandle / EventLoop

	/// <summary>O(1)-доступ к stage-handle для reverse-link <see cref="IInstanceHandle.Stage"/>.</summary>
	internal StageHandle GetStageHandle(StageDescriptor stage) => _stageHandles[stage.Name];

	internal IReadOnlyCollection<Instance> InstancesOf(StageDescriptor stage) => _instances.InstancesOf(stage);

	internal Instance? FindInstance(InstanceIdentity identity) => _instances.Find(identity);

	private void OnInstanceAdded(Instance instance) {
		if (_stageHandles.TryGetValue(instance.Stage.Name, out var handle)) {
			handle.NotifyAdded(instance);
		}
	}

	private void OnInstanceRemoved(Instance instance) {
		if (_stageHandles.TryGetValue(instance.Stage.Name, out var handle)) {
			handle.NotifyRemoved(instance);
		}
		// При cascade-removal завершаем iteration-broadcaster — consumer'ы инстанса получат естественный exit.
		instance.CompleteIterationSubscribers();
	}

	/// <summary>
	/// Hosted-service-side hook на финальный shutdown. Завершает все висящие broadcaster-каналы
	/// (<see cref="IStageHandle.Changes"/> и <see cref="IInstanceHandle"/>-as-AsyncEnumerable), чтобы
	/// consumer'ы без явно переданного <see cref="CancellationToken"/> вышли из <c>await foreach</c>
	/// естественно. Для инстансов, удалённых каскадом, iteration-subscribers уже completed в
	/// <see cref="OnInstanceRemoved"/>; здесь — для idle-инстансов, переживших shutdown.
	/// </summary>
	internal void OnShutdown() {
		foreach (var stage in _stageHandles.Values) stage.CompleteAllSubscribers();
		foreach (var instance in _instances.All) instance.CompleteIterationSubscribers();
	}

	/// <summary>
	/// Identity-based RunAsync: identity уже валидирован в StageHandle-indexer (проверка key-names
	/// против ExpectedKeyNames), поэтому валидация ключей здесь не повторяется.
	/// </summary>
	internal async Task<IIterationHandle> RunAsync(InstanceIdentity identity, CancellationToken ct = default) {
		if (_lifecycle.IsFaulted) {
			Log.TriggerFaulted(_logger, identity.Stage.Name, null);
			throw new IterationRejectedException(IterationRejectReason.Faulted, identity.FullyQualifiedName,
				$"Запуск инстанса {identity.FullyQualifiedName} невозможен: оркестратор в Faulted-состоянии.");
		}
		var tcs = new TaskCompletionSource<IIterationHandle>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(identity, tcs);
		try {
			await _channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			throw new IterationRejectedException(IterationRejectReason.Faulted, identity.FullyQualifiedName,
				$"Запуск инстанса {identity.FullyQualifiedName} невозможен: channel оркестратора закрыт.");
		}
		await using var ctReg = ct.UnsafeRegister(static state => {
			var t = (TaskCompletionSource<IIterationHandle>)state!;
			t.TrySetCanceled();
		}, tcs).ConfigureAwait(false);
		var handle = await tcs.Task.ConfigureAwait(false);
		Log.IterationStarted(_logger, identity.Stage.Name, null);
		return handle;
	}

	/// <summary>
	/// Внешний bootstrap keyspace для keyless-эмиттера. Работает только для стадий без
	/// <c>DependsOnInstance</c>-зависимостей.
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

	private Instance ResolveKeylessSource(StageDescriptor stage) {
		if (stage.ExpectedKeyNames.Count != 0) {
			throw new InvalidOperationException(
				$"Стадия '{stage.Name}' имеет ключевые зависимости. Внешний RegisterKey/UnregisterKey работает только для keyless-эмиттеров; используйте JobContext.AddKeyAsync/RemoveKeyAsync из ExecuteAsync.");
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
	/// </summary>
	private static class Log {
		public static readonly Action<ILogger, string, Exception?> IterationStarted =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(6001, nameof(IterationStarted)),
				"RunAsync({StageName}) → итерация запущена");

		public static readonly Action<ILogger, string, Exception?> TriggerFaulted =
			LoggerMessage.Define<string>(LogLevel.Debug, new EventId(6002, nameof(TriggerFaulted)),
				"RunAsync({StageName}) → Faulted (оркестратор крашнулся)");

		public static readonly Action<ILogger, string, string, Exception?> RegisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6005, nameof(RegisterKeyExternal)),
				"RegisterKey(stage={StageName}, key={Key}) внешний вызов (bootstrap)");

		public static readonly Action<ILogger, string, string, Exception?> UnregisterKeyExternal =
			LoggerMessage.Define<string, string>(LogLevel.Information, new EventId(6006, nameof(UnregisterKeyExternal)),
				"UnregisterKey(stage={StageName}, key={Key}) внешний вызов");
	}
}
