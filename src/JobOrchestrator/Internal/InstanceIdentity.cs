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
/// <para>
/// <b>Perf-инвариант:</b> при создании Identity входящие <c>dependencyKeys</c> сортируются один раз
/// по <see cref="StageDescriptor.ExpectedKeyNames"/>; <see cref="EncodedKey"/> и
/// <see cref="FullyQualifiedName"/> строятся из уже-упорядоченного списка без <c>OrderBy</c>.
/// Hash-code кешируется при первом чтении (Identity — частый Dictionary-key в reg-ах).
/// </para>
/// </summary>
internal sealed class InstanceIdentity : IEquatable<InstanceIdentity> {
	public StageDescriptor Stage { get; }

	/// <summary>Композитный ключ инстанса. ImmutableDictionary — защита от случайной мутации callers'ом.</summary>
	public ImmutableDictionary<string, string> DependencyKeys { get; }

	/// <summary>
	/// Канонический encoding ключа (порядок задаётся <see cref="StageDescriptor.ExpectedKeyNames"/>, escaped) —
	/// используется как hash-key в <see cref="InstanceManager"/>, <see cref="KeyspaceRegistry"/> и waiters.
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

	// Lazy-cached hash. 0 = не вычислен (значение 0 заменяется на 1, чтобы не путаться с sentinel).
	// Безопасно гонкам: разные потоки могут перевычислять детерминированное значение, последняя запись
	// атомарна для int.
	private int _hashCode;

	/// <summary>
	/// <paramref name="dependencyKeys"/> опционален: <c>null</c> = пустой словарь (для keyless-стадий —
	/// типичный путь, чтобы не плодить <c>new Dictionary&lt;string,string&gt;(StringComparer.Ordinal)</c>
	/// на каждом call-site).
	/// </summary>
	public InstanceIdentity(StageDescriptor stage, IReadOnlyDictionary<string, string>? dependencyKeys = null) {
		ArgumentNullException.ThrowIfNull(stage);
		Stage = stage;
		DependencyKeys = dependencyKeys switch {
			null => EmptyKeys,
			ImmutableDictionary<string, string> already => already,
			_ => ImmutableDictionary.CreateRange(StringComparer.Ordinal, dependencyKeys),
		};

		// Сортируем один раз по ExpectedKeyNames; Encode и FormatFqn итерируют уже-упорядоченный
		// массив без вторичного OrderBy. Safety-net хвост — ключи, отсутствующие в ExpectedKeyNames
		// (теоретически невозможно для валидных инстансов, но не глотаем silently).
		var ordered = OrderEntries(DependencyKeys, stage.ExpectedKeyNames);
		EncodedKey = EncodeOrdered(ordered);
		FullyQualifiedName = FormatFqnOrdered(stage.Name, ordered);
	}

	private static readonly ImmutableDictionary<string, string> EmptyKeys =
		ImmutableDictionary<string, string>.Empty.WithComparers(StringComparer.Ordinal);

	/// <summary>
	/// Упорядочивает <paramref name="keys"/> по <paramref name="expectedKeyNames"/>: сначала найденные
	/// в порядке expectedKeyNames, затем хвостом — те, которых нет в ExpectedKeyNames (safety-net).
	/// Возвращает Pool-friendly <c>KeyValuePair[]</c> (struct, no boxing).
	/// </summary>
	private static KeyValuePair<string, string>[] OrderEntries(
		ImmutableDictionary<string, string> keys,
		IReadOnlyList<string> expectedKeyNames
	) {
		if (keys.Count == 0) return [];
		var ordered = new KeyValuePair<string, string>[keys.Count];
		int idx = 0;
		var seen = new HashSet<string>(StringComparer.Ordinal);
		foreach (var name in expectedKeyNames) {
			if (keys.TryGetValue(name, out var v)) {
				ordered[idx++] = new KeyValuePair<string, string>(name, v);
				seen.Add(name);
			}
		}
		foreach (var kv in keys) {
			if (!seen.Contains(kv.Key)) ordered[idx++] = kv;
		}
		return ordered;
	}

	/// <summary>
	/// Канонический encoding из уже-упорядоченных entries. Спецсимволы (<c>|</c>, <c>=</c>, <c>\</c>)
	/// экранируются обратным слэшем, чтобы исключить collision-возможность вида <c>{a: "1|b=2"}</c>
	/// vs <c>{a: "1", b: "2"}</c>.
	/// <para>
	/// Канонично per-stage: два Identity для одной стадии с одинаковыми key-value-парами получают
	/// одинаковый EncodedKey, потому что ExpectedKeyNames-порядок одинаковый.
	/// </para>
	/// </summary>
	private static string EncodeOrdered(KeyValuePair<string, string>[] ordered) {
		if (ordered.Length == 0) return string.Empty;
		var sb = new StringBuilder();
		foreach (var kv in ordered) {
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
	/// Human-readable FQN: <c>"stage[dep1=v1,dep2=v2,...]"</c> в порядке pre-sorted entries.
	/// Тот же порядок, что и в <see cref="EncodedKey"/> — внутренне консистентно.
	/// </summary>
	private static string FormatFqnOrdered(string stageName, KeyValuePair<string, string>[] ordered) {
		if (ordered.Length == 0) return $"{stageName}[]";
		var parts = new string[ordered.Length];
		for (int i = 0; i < ordered.Length; i++) parts[i] = $"{ordered[i].Key}={ordered[i].Value}";
		return $"{stageName}[{string.Join(",", parts)}]";
	}

	public bool Equals(InstanceIdentity? other) =>
		other is not null
		&& string.Equals(Stage.Name, other.Stage.Name, StringComparison.Ordinal)
		&& string.Equals(EncodedKey, other.EncodedKey, StringComparison.Ordinal);

	public override bool Equals(object? obj) => obj is InstanceIdentity id && Equals(id);

	public override int GetHashCode() {
		// Lazy-cache: первое чтение вычисляет, последующие — атомарный read. Race detected → safe
		// (детерминированное значение, повторное вычисление даёт то же).
		if (_hashCode != 0) return _hashCode;
		var hash = HashCode.Combine(Stage.Name, EncodedKey);
		// 0 — sentinel «не вычислен»; маппим в 1, чтобы избежать ложного re-compute.
		_hashCode = hash == 0 ? 1 : hash;
		return _hashCode;
	}

	public override string ToString() => FullyQualifiedName;
}
