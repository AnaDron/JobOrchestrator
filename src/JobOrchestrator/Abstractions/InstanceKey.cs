namespace JobOrchestrator.Abstractions;

/// <summary>
/// Typed sentinel для <see cref="IStageHandle"/>-индексатора, означающий «keyless-инстанс»
/// (стадия без <c>DependsOnInstance</c>-зависимостей). Используется как
/// <c>orchestrator["shops"][InstanceKey.None].RunAsync()</c> — это даёт синтаксическую
/// униформность: каждый instance-lookup идёт через <c>[...]</c>-индексатор, без разделения
/// на «keyless property» и «keyed indexer».
/// </summary>
public enum InstanceKey {
	/// <summary>Маркер «no dependency keys» — для безключевых стадий.</summary>
	None,
}
