using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IJobOrchestrator"/>: фасад поверх event loop'а.
/// <list type="bullet">
/// <item><c>TriggerAsync</c>: валидация ключей → публикация <see cref="ManualTriggerRequestedEvent"/> в Channel, await TCS.</item>
/// <item><c>Register/UnregisterKey</c>: проверка существования стадии → публикация <see cref="KeyAddedEvent"/>/<see cref="KeyRemovedEvent"/>.</item>
/// <item><c>GetOverview</c>: синхронный snapshot через atomic-reads <see cref="Instance"/>-полей. Lock-free, без RPC. Доступен и в Faulted (для диагностики).</item>
/// </list>
/// </summary>
internal sealed class JobOrchestratorRuntime(
	Channel<OrchestratorEvent> channel,
	StageRegistry registry,
	InstanceManager instances,
	OrchestratorLifecycle lifecycle,
	SuccessWaiters successWaiters,
	OutcomeWaiters outcomeWaiters,
	ILogger<JobOrchestratorRuntime> logger
) : IJobOrchestrator {
	public bool IsFaulted => lifecycle.IsFaulted;

	public async Task<TriggerResult> TriggerAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (lifecycle.IsFaulted) {
			Log.TriggerFaulted(logger, stageName, null);
			return TriggerResult.Faulted;
		}
		if (!registry.TryGet(stageName, out var stage)) {
			Log.TriggerNotFound(logger, stageName, null);
			return TriggerResult.NotFound;
		}

		if (!ValidateKeys(stage!.ExpectedKeyNames, dependencyKeys)) {
			Log.TriggerInvalidKeys(logger, stageName, null);
			return TriggerResult.InvalidKeys;
		}

		// Identity строится ОДИН РАЗ здесь (после валидации) — encode + format происходят на этом
		// единственном вызове, а не в каждом EventLoop.HandleManualTrigger через Find(stage, keys).
		var identity = new InstanceIdentity(stage, dependencyKeys);
		var tcs = new TaskCompletionSource<TriggerResult>(TaskCreationOptions.RunContinuationsAsynchronously);
		var evt = new ManualTriggerRequestedEvent(identity, tcs);
		try {
			await channel.Writer.WriteAsync(evt, ct).ConfigureAwait(false);
		} catch (ChannelClosedException) {
			return TriggerResult.Faulted;
		}
		var result = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
		Log.TriggerCompleted(logger, stageName, result, null);
		return result;
	}

	public void RegisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		Log.RegisterKeyExternal(logger, stageName, key, null);
		channel.Writer.Publish(new KeyAddedEvent(source, key));
	}

	public void UnregisterKey(string stageName, string key) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		ArgumentException.ThrowIfNullOrEmpty(key);
		ThrowIfFaulted();
		var source = ResolveKeylessSource(stageName);
		Log.UnregisterKeyExternal(logger, stageName, key, null);
		channel.Writer.Publish(new KeyRemovedEvent(source, key));
	}

	/// <summary>
	/// Резолвит keyless-инстанс стадии для внешнего <see cref="RegisterKey"/>/<see cref="UnregisterKey"/>.
	/// Работает только для стадий без <c>DependsOnInstance</c>-зависимостей (там нет ambiguity, какой
	/// инстанс-эмитер использовать как Source). Для ключевых стадий — InvalidOperationException
	/// (BL должна использовать <see cref="JobContext.AddKey"/> изнутри сервиса).
	/// </summary>
	private Instance ResolveKeylessSource(string stageName) {
		if (!registry.TryGet(stageName, out var stage)) {
			throw new ArgumentException($"Стадия '{stageName}' не зарегистрирована в графе.", nameof(stageName));
		}
		if (stage!.ExpectedKeyNames.Count != 0) {
			throw new InvalidOperationException(
				$"Стадия '{stageName}' имеет ключевые зависимости. Внешний RegisterKey/UnregisterKey работает только для keyless-эмитеров; используйте JobContext.AddKey/RemoveKey из ExecuteAsync.");
		}
		var source = instances.Find(new InstanceIdentity(stage));
		if (source is null) {
			throw new InvalidOperationException(
				$"Keyless-инстанс для стадии '{stageName}' ещё не создан (оркестратор не стартован?).");
		}
		return source;
	}

	/// <summary>
	/// Lock-free snapshot. Доступен и в <see cref="IsFaulted"/>-состоянии — для post-mortem-диагностики
	/// важно видеть, в каком состоянии оркестратор крашнулся (метрики, расписания инстансов).
	/// </summary>
	public InstancesOverview GetOverview() => instances.Snapshot();

	public Task WaitForStageSuccessAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		var identity = ResolveIdentity(stageName, dependencyKeys);
		var task = successWaiters.Register(identity.Stage.Name, identity.EncodedKey, ct);

		// Fast-path: если инстанс УЖЕ существует И УЖЕ имел success — сразу резолвим (без ожидания
		// следующего цикла). SignalSuccess идемпотентна — повторный сигнал на already-Completed bucket — no-op.
		var existing = instances.Find(identity);
		if (existing is not null
			&& existing.State != InstanceLifecycleState.Terminating
			&& existing.Metrics.LastSuccess is not null) {
			successWaiters.SignalSuccess(identity.Stage.Name, identity.EncodedKey);
		}

		return task;
	}

	public Task<StageOutcome> WaitForStageOutcomeAsync(
		string stageName,
		IReadOnlyDictionary<string, string>? dependencyKeys = null,
		CancellationToken ct = default
	) {
		var identity = ResolveIdentity(stageName, dependencyKeys);
		// Outcome НЕ имеет fast-path: семантика «исход СЛЕДУЮЩЕГО цикла». Caller сам отвечает за
		// порядок «Register → Trigger». Memoized только если Signal случился ПОСЛЕ старта оркестратора
		// и ДО Register'а — late-register получит последний outcome.
		return outcomeWaiters.Register(identity.Stage.Name, identity.EncodedKey, ct);
	}

	/// <summary>
	/// Резолвит <see cref="InstanceIdentity"/> с валидацией: stage существует в графе, набор ключей
	/// соответствует <see cref="StageRegistry.ExpectedKeyNames"/>. Используется WaitFor-методами,
	/// чтобы регистрация валидной identity всегда могла дождаться сигнала (даже если инстанса ещё нет).
	/// </summary>
	private InstanceIdentity ResolveIdentity(string stageName, IReadOnlyDictionary<string, string>? dependencyKeys) {
		ArgumentException.ThrowIfNullOrEmpty(stageName);
		if (!registry.TryGet(stageName, out var stage)) {
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
		if (lifecycle.IsFaulted) {
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
