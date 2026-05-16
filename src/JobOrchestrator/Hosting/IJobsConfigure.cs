namespace JobOrchestrator.Hosting;

/// <summary>
/// Элемент accumulator-pattern: каждый <see cref="ServiceCollectionExtensions.AddJobOrchestrator"/>-вызов
/// регистрирует один <see cref="IJobsConfigure"/> как multi-singleton. При первом resolve'е
/// <see cref="Internal.StageRegistry"/> все накопленные элементы применяются к shared
/// <see cref="JobOrchestratorBuilder"/> в порядке регистрации.
/// </summary>
internal interface IJobsConfigure {
	/// <summary>Применяет накопленную configure-action к общему builder'у.</summary>
	void Apply(JobOrchestratorBuilder builder);
}

/// <summary>
/// Реализация <see cref="IJobsConfigure"/>: оборачивает <see cref="Action{T}"/>, переданный в
/// <see cref="ServiceCollectionExtensions.AddJobOrchestrator"/>.
/// </summary>
internal sealed class JobsConfigure(Action<JobOrchestratorBuilder> configure) : IJobsConfigure {
	public void Apply(JobOrchestratorBuilder builder) => configure(builder);
}
