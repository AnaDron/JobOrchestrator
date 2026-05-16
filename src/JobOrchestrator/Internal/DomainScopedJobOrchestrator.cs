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
internal sealed record DomainScopedJobOrchestrator(JobOrchestratorRuntime Runtime, string Domain) : IDomainScopedJobOrchestrator {
	public IStageHandle this[string stageName] {
		get {
			ArgumentException.ThrowIfNullOrEmpty(stageName);
			return Runtime[$"{Domain}{JobOrchestratorBuilder.DomainSeparator}{stageName}"];
		}
	}
}
