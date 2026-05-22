namespace JobOrchestrator.Abstractions;

/// <summary>
/// Событие жизненного цикла keyspace стадии — добавление или удаление инстанса. Поток получается через
/// <see cref="IStageHandle.Changes"/>: подписчик при подписке сначала видит <see cref="StageChangeKind.Added"/>
/// для каждого инстанса, существующего на момент подписки (replay), затем live-поток последующих изменений.
/// </summary>
/// <param name="Instance">Identity-handle затронутого инстанса.</param>
/// <param name="Kind">Тип изменения.</param>
public sealed record StageChange(IInstanceHandle Instance, StageChangeKind Kind);
