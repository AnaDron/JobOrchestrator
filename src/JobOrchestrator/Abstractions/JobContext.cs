namespace JobOrchestrator.Abstractions;

/// <summary>
/// Контекст итерации инстанса стадии. Создаётся SDK перед вызовом <see cref="IJobService.ExecuteAsync"/>.
/// </summary>
/// <remarks>
/// <para>
/// Все обязательные поля помечены <c>required init</c>: SDK конструирует <see cref="JobContext"/>
/// через object-initializer в <c>StageRunner</c>; пользовательский unit-тест сервиса может построить
/// собственный <see cref="JobContext"/> с тестовым <see cref="IJobContextSink"/> для проверки,
/// что сервис корректно вызывает <see cref="AddKey"/>/<see cref="RemoveKey"/>.
/// </para>
/// <para>
/// Фасадные методы <see cref="AddKey"/>/<see cref="RemoveKey"/> остаются на самом контексте
/// (<c>ctx.AddKey(k)</c> короче, чем <c>ctx.Sink.AddKey(k)</c>); внутри делегируют в <see cref="Sink"/>.
/// </para>
/// </remarks>
public sealed class JobContext {
	/// <summary>Уникальный идентификатор сквозной диагностики (логи/трассировка) для текущей итерации.</summary>
	public required string CorrelationId { get; init; }

	/// <summary>Источник, вызвавший эту итерацию: <see cref="TriggerSource.Auto"/> (таймер) или <see cref="TriggerSource.Manual"/>.</summary>
	public required TriggerSource Trigger { get; init; }

	/// <summary>Персистентное состояние инстанса (scope изолирован per-инстанс автоматически).</summary>
	public required IJobState State { get; init; }

	/// <summary>
	/// Время последнего успешного завершения этого инстанса или <c>null</c>, если этот инстанс
	/// ещё ни разу не был успешен. Не сбрасывается на последующих неуспехах — монотонная метка.
	/// БЛ может использовать для решений «full vs delta sync» (<c>LastSuccessAt == null</c> ⇔ первый запуск).
	/// </summary>
	public DateTimeOffset? LastSuccessAt { get; init; }

	/// <summary>
	/// Композитный ключ инстанса: для каждой <c>DependsOnInstance(X)</c> этой стадии — парный ключ из keyspace(X),
	/// плюс ключи, унаследованные через <c>DependsOn(Y)</c> от инстансов вышестоящих стадий.
	/// Для безключевой стадии — пустой словарь.
	/// </summary>
	public required IReadOnlyDictionary<string, string> DependencyKeys { get; init; }

	/// <summary>
	/// Имя инстанса для логирования: <c>"stageName[dep1=val1,dep2=val2,...]"</c> без пробелов после запятых,
	/// с компонентами в порядке объявления зависимостей в Fluent API.
	/// </summary>
	public required string FullyQualifiedName { get; init; }

	/// <summary>
	/// Канал распространения <see cref="AddKey"/>/<see cref="RemoveKey"/> наружу. В production —
	/// внутренняя реализация, публикующая события в event loop. В тестах можно подменить на mock.
	/// </summary>
	public required IJobContextSink Sink { get; init; }

	/// <summary>
	/// Регистрирует ключ в keyspace ТЕКУЩЕЙ стадии. Идемпотентно: повторный вызов с тем же ключом — no-op.
	/// Не блокирует — публикует событие в event loop, обработка асинхронная. Фасад над <see cref="Sink"/>.
	/// </summary>
	public void AddKey(string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		Sink.AddKey(key);
	}

	/// <summary>
	/// Удаляет ключ из keyspace ТЕКУЩЕЙ стадии. Каскадно gracefully cancel-ит инстансы зависимых стадий
	/// с этим компонентом ключа, в топологически обратном порядке (листья перед корнями). Фасад над <see cref="Sink"/>.
	/// </summary>
	public void RemoveKey(string key) {
		ArgumentException.ThrowIfNullOrEmpty(key);
		Sink.RemoveKey(key);
	}
}
