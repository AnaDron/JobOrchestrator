namespace JobOrchestrator.Abstractions;

/// <summary>
/// Контекст итерации инстанса стадии. Создаётся SDK перед вызовом <see cref="IJobService.ExecuteAsync"/>.
/// </summary>
/// <remarks>
/// Конструктор <c>internal</c> — пользовательский код не создаёт <see cref="JobContext"/> напрямую,
/// SDK поставляет его в каждую итерацию. Тестовые сборки имеют доступ через <c>InternalsVisibleTo</c>.
/// </remarks>
public sealed class JobContext {
	private readonly Action<string>? _addKey;
	private readonly Action<string>? _removeKey;

	internal JobContext(
		string correlationId,
		TriggerSource trigger,
		IJobState state,
		DateTimeOffset? lastSuccessAt,
		IReadOnlyDictionary<string, string> dependencyKeys,
		string fullyQualifiedName,
		Action<string>? addKey,
		Action<string>? removeKey
	) {
		CorrelationId = correlationId;
		Trigger = trigger;
		State = state;
		LastSuccessAt = lastSuccessAt;
		DependencyKeys = dependencyKeys;
		FullyQualifiedName = fullyQualifiedName;
		_addKey = addKey;
		_removeKey = removeKey;
	}

	/// <summary>Уникальный идентификатор сквозной диагностики (логи/трассировка) для текущей итерации.</summary>
	public string CorrelationId { get; }

	/// <summary>Источник, вызвавший эту итерацию: <see cref="TriggerSource.Auto"/> (таймер) или <see cref="TriggerSource.Manual"/>.</summary>
	public TriggerSource Trigger { get; }

	/// <summary>Персистентное состояние инстанса (scope изолирован per-инстанс автоматически).</summary>
	public IJobState State { get; }

	/// <summary>
	/// Время последнего успешного завершения этого инстанса или <c>null</c>, если этот инстанс
	/// ещё ни разу не был успешен. Не сбрасывается на последующих неуспехах — монотонная метка.
	/// БЛ может использовать для решений «full vs delta sync» (<c>LastSuccessAt == null</c> ⇔ первый запуск).
	/// </summary>
	public DateTimeOffset? LastSuccessAt { get; }

	/// <summary>
	/// Композитный ключ инстанса: для каждой <c>DependsOnInstance(X)</c> этой стадии — парный ключ из keyspace(X),
	/// плюс ключи, унаследованные через <c>DependsOn(Y)</c> от инстансов вышестоящих стадий.
	/// Для безключевой стадии — пустой словарь.
	/// </summary>
	public IReadOnlyDictionary<string, string> DependencyKeys { get; }

	/// <summary>
	/// Имя инстанса для логирования: <c>"stageName[dep1=val1,dep2=val2,...]"</c> без пробелов после запятых,
	/// с компонентами в порядке объявления зависимостей в Fluent API. SDK кладёт это значение
	/// в <c>logger.BeginScope</c>-поля рядом со структурными компонентами ключа.
	/// </summary>
	public string FullyQualifiedName { get; }

	/// <summary>
	/// Регистрирует ключ в keyspace ТЕКУЩЕЙ стадии. Идемпотентно: повторный вызов с тем же ключом — no-op.
	/// Не блокирует — публикует событие в event loop, обработка асинхронная. Если зависимые стадии
	/// имеют <c>DependsOnInstance</c> на текущую стадию и все их остальные зависимости разрешены —
	/// SDK немедленно создаст их инстансы и поставит в очередь.
	/// </summary>
	public void AddKey(string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		_addKey?.Invoke(key);
	}

	/// <summary>
	/// Удаляет ключ из keyspace ТЕКУЩЕЙ стадии. Идемпотентно: отсутствующий ключ — no-op.
	/// Каскадно gracefully cancel-ит инстансы зависимых стадий с этим компонентом ключа,
	/// в топологически обратном порядке (листья перед корнями графа).
	/// </summary>
	public void RemoveKey(string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		_removeKey?.Invoke(key);
	}
}
