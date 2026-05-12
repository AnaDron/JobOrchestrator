namespace JobOrchestrator.Abstractions;

/// <summary>
/// Реализация стадии. Резолвится из свежего DI-scope на каждую итерацию.
/// </summary>
/// <remarks>
/// Контракт итерации:
/// <list type="bullet">
/// <item>Должен завершиться (или бросить исключение) до истечения watchdog-таймаута, заданного через <c>IStageBuilder.WithExecutionTimeout</c>.</item>
/// <item>Внутренние повторы при ошибках (HTTP-retry и т. п.) — забота БЛ; SDK реагирует только на финальный исход (return vs throw).</item>
/// <item>Не оборачивает итерацию в транзакцию — это забота БЛ через scoped-сервисы DI.</item>
/// <item>Может вызывать <see cref="JobContext.AddKey"/>/<see cref="JobContext.RemoveKey"/> любое число раз — события публикуются асинхронно в event loop, метод возвращается мгновенно.</item>
/// </list>
/// </remarks>
public interface IJobService {
	/// <summary>Выполнить одну итерацию для текущего инстанса.</summary>
	Task ExecuteAsync(JobContext ctx, CancellationToken ct);
}
