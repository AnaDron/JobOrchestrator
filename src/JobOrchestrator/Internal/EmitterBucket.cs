namespace JobOrchestrator.Internal;

/// <summary>
/// Bucket эмитера: его <see cref="InstanceIdentity"/> (через который доступны Stage и DependencyKeys
/// для cross-merge при создании зависимых инстансов) + Keys, которые он опубликовал через
/// <see cref="JobContext.AddKeyAsync"/> или внешний <see cref="IStageHandle.RegisterKey"/>.
/// </summary>
internal sealed record EmitterBucket(InstanceIdentity Emitter, HashSet<string> Keys);
