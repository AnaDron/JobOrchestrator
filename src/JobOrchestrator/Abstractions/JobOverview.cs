namespace JobOrchestrator.Abstractions;

/// <summary>Диагностический снимок всех существующих инстансов всех стадий.</summary>
public sealed record JobOverview(IReadOnlyList<JobInfo> Jobs);
