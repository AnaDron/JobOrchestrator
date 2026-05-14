using System.Collections.Immutable;
using System.Text;

namespace JobOrchestrator.Internal;

/// <summary>
/// Иммутабельный композитный идентификатор инстанса стадии: <c>(Stage, DependencyKeys)</c>.
/// Single source of truth для encoded-key и FullyQualifiedName — никто снаружи не должен строить
/// эти строки по своим правилам (риск рассинхронизации форматов между call-sites).
/// <para>
/// Используется как «ключ-идентификатор» инстанса по всему SDK:
/// </para>
/// <list type="bullet">
/// <item><see cref="InstanceManager"/> хранит инстансы по <c>Identity</c>;</item>
/// <item>Channel-события (<c>TimerTickedEvent</c>, <c>StageCompletedEvent</c>, …) несут ссылку на <see cref="Instance"/>, у которого есть <c>Identity</c>;</item>
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
	/// Human-readable идентификатор: <c>"stage[dep1=v1,dep2=v2,...]"</c> в порядке
	/// <see cref="StageDescriptor.ExpectedKeyNames"/> (= порядок Fluent-API объявлений транзитивно
	/// через цепочку зависимостей). Стабилен и детерминирован независимо от hash-order
	/// <see cref="DependencyKeys"/>.
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
		EncodedKey = Encode(DependencyKeys);
		FullyQualifiedName = FormatFqn(stage.Name, DependencyKeys, stage.ExpectedKeyNames);
	}

	/// <summary>
	/// Канонический encoding: компоненты отсортированы по имени; спецсимволы (<c>|</c>, <c>=</c>, <c>\</c>) экранируются
	/// обратным слэшем, чтобы исключить collision-возможность вида <c>{a: "1|b=2"}</c> vs <c>{a: "1", b: "2"}</c>.
	/// <para>
	/// Приватная деталь Identity: encoded-key — это hash-key для <see cref="InstanceManager"/> и
	/// <see cref="KeyspaceRegistry"/>. После Phase C рефакторинга это единственная точка кодирования —
	/// внешний <c>DependencyKey.Encode</c>-utility удалён.
	/// </para>
	/// </summary>
	internal static string Encode(IReadOnlyDictionary<string, string> keys) {
		if (keys.Count == 0) return string.Empty;
		var sb = new StringBuilder();
		foreach (var kv in keys.OrderBy(kv => kv.Key, StringComparer.Ordinal)) {
			if (sb.Length > 0) sb.Append('|');
			AppendEscaped(sb, kv.Key);
			sb.Append('=');
			AppendEscaped(sb, kv.Value);
		}
		return sb.ToString();
	}

	private static void AppendEscaped(StringBuilder sb, string value) {
		foreach (char c in value) {
			if (c is '\\' or '|' or '=') sb.Append('\\');
			sb.Append(c);
		}
	}

	/// <summary>
	/// Human-readable FQN: <c>"stage[dep1=v1,dep2=v2,...]"</c>. Порядок компонентов задаётся
	/// <paramref name="orderedKeyNames"/> (= <see cref="StageDescriptor.ExpectedKeyNames"/>) —
	/// детерминированный независимо от hash-table-order'а <c>ImmutableDictionary</c>.
	/// </summary>
	private static string FormatFqn(string stageName, ImmutableDictionary<string, string> keys, IReadOnlyList<string> orderedKeyNames) {
		if (keys.Count == 0) return $"{stageName}[]";
		var seen = new HashSet<string>(StringComparer.Ordinal);
		var parts = new List<string>(keys.Count);
		foreach (var name in orderedKeyNames) {
			if (keys.TryGetValue(name, out var v)) {
				parts.Add($"{name}={v}");
				seen.Add(name);
			}
		}
		// Safety net: keys, отсутствующие в orderedKeyNames (теоретически невозможно для валидных инстансов).
		foreach (var kv in keys) {
			if (!seen.Contains(kv.Key)) parts.Add($"{kv.Key}={kv.Value}");
		}
		return $"{stageName}[{string.Join(",", parts)}]";
	}

	public bool Equals(InstanceIdentity? other) =>
		other is not null
		&& string.Equals(Stage.Name, other.Stage.Name, StringComparison.Ordinal)
		&& string.Equals(EncodedKey, other.EncodedKey, StringComparison.Ordinal);

	public override bool Equals(object? obj) => obj is InstanceIdentity id && Equals(id);

	public override int GetHashCode() => HashCode.Combine(Stage.Name, EncodedKey);

	public override string ToString() => FullyQualifiedName;
}
