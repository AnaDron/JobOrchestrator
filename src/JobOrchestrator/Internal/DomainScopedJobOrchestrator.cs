namespace JobOrchestrator.Internal;

/// <summary>
/// Реализация <see cref="IDomainScopedJobOrchestrator"/>: тонкий decorator над
/// <see cref="JobOrchestratorRuntime"/>, который добавляет <c>"{Domain}:"</c>-префикс к имени стадии
/// в indexer'е.
/// <para>
/// Lifetime — кэшируется per-domain в <see cref="JobOrchestratorRuntime"/>, поэтому повторные
/// вызовы <c>WithDomain</c> с одинаковым ключом возвращают тот же объект.
/// </para>
/// </summary>
internal sealed class DomainScopedJobOrchestrator(JobOrchestratorRuntime runtime, string domain)
	: IDomainScopedJobOrchestrator {
	public IStageHandle this[string stageName] {
		get {
			ArgumentException.ThrowIfNullOrEmpty(stageName);
			return runtime[$"{domain}{JobOrchestratorBuilder.DomainSeparator}{stageName}"];
		}
	}
}
