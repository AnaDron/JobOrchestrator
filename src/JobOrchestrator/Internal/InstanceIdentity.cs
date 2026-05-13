using System.Collections.Immutable;

namespace JobOrchestrator.Internal;

/// <summary>
/// Иммутабельный композитный идентификатор инстанса стадии: <c>(Stage, DependencyKeys)</c>.
/// Внутри пред-вычисляет <see cref="EncodedKey"/> (canonical для словарных lookup-ов) и
/// <see cref="FullyQualifiedName"/> (human-readable для логов).
/// <para>
/// Используется как «ключ-идентификатор» инстанса по всему SDK:
/// </para>
/// <list type="bullet">
/// <item><see cref="InstanceManager"/> хранит инстансы по <c>Identity</c>;</item>
/// <item>Channel-события (<c>TimerTickedEvent</c>, <c>StageCompletedEvent</c>, …) несут ссылку на <see cref="StageInstance"/>, у которого есть <c>Identity</c>;</item>
/// <item><see cref="JobContext.FullyQualifiedName"/>/<c>DependencyKeys</c> — фасады поверх Identity.</item>
/// </list>
/// <para>
/// Equality по <c>(Stage.Name, EncodedKey)</c> — два Identity с одинаковыми Stage и набором ключей считаются равными.
/// </para>
/// </summary>
internal sealed class InstanceIdentity : IEquatable<InstanceIdentity> {
	public StageDescriptor Stage { get; }

	/// <summary>Композитный ключ инстанса. ImmutableDictionary — защита от случайной мутации callers'ом.</summary>
	public ImmutableDictionary<string, string> DependencyKeys { get; }

	/// <summary>
	/// Канонический encoding ключа (sort by name, escaped) — используется как hash-key в <see cref="InstanceManager"/>.
	/// </summary>
	public string EncodedKey { get; }

	/// <summary>
	/// Human-readable идентификатор: <c>"stage[dep1=v1,dep2=v2,...]"</c> в порядке объявления зависимостей.
	/// </summary>
	public string FullyQualifiedName { get; }

	/// <summary>Scope для <see cref="IJobStateStore"/>: <c>"{StageName}:{EncodedKey}"</c>.</summary>
	public string StateScope => $"{Stage.Name}:{EncodedKey}";

	public InstanceIdentity(StageDescriptor stage, IReadOnlyDictionary<string, string> dependencyKeys) {
		ArgumentNullException.ThrowIfNull(stage);
		ArgumentNullException.ThrowIfNull(dependencyKeys);
		Stage = stage;
		DependencyKeys = dependencyKeys as ImmutableDictionary<string, string>
			?? ImmutableDictionary.CreateRange(StringComparer.Ordinal, dependencyKeys);
		EncodedKey = DependencyKey.Encode(DependencyKeys);
		FullyQualifiedName = DependencyKey.FormatFullyQualifiedName(stage.Name, DependencyKeys);
	}

	public bool Equals(InstanceIdentity? other) =>
		other is not null
		&& string.Equals(Stage.Name, other.Stage.Name, StringComparison.Ordinal)
		&& string.Equals(EncodedKey, other.EncodedKey, StringComparison.Ordinal);

	public override bool Equals(object? obj) => obj is InstanceIdentity id && Equals(id);

	public override int GetHashCode() => HashCode.Combine(Stage.Name, EncodedKey);

	public override string ToString() => FullyQualifiedName;
}
