namespace JobOrchestrator.Abstractions;

/// <summary>Диагностический снимок всех существующих инстансов всех стадий.</summary>
public sealed record InstancesOverview(IReadOnlyList<InstanceInfo> Instances);
