namespace JobOrchestrator.Abstractions;

/// <summary>
/// Domain-проекция оркестратора: indexer <c>[stageName]</c> добавляет domain-префикс к имени
/// перед делегированием в <see cref="IJobOrchestrator.this[string]"/>.
/// <para>
/// Получается через <see cref="IJobOrchestrator.WithDomain(string)"/>; lifetime — singleton, кэшируется
/// per-domain внутри <see cref="IJobOrchestrator"/>-реализации (без per-call аллокаций).
/// </para>
/// <para>
/// Эквивалентность: <c>orchestrator.WithDomain("evotor")["shops"]</c> == <c>orchestrator["evotor:shops"]</c>.
/// </para>
/// </summary>
public interface IDomainScopedJobOrchestrator {
	/// <summary>
	/// Доступ к стадии по короткому имени (без domain-префикса). Реальный resolve идёт по
	/// полному имени <c>"{domain}:{stageName}"</c>.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадия с полным domain-prefixed именем не зарегистрирована.</exception>
	IStageHandle this[string stageName] { get; }
}
