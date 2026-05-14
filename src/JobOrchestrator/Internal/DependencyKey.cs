using System.Text;

namespace JobOrchestrator.Internal;

/// <summary>
/// Утилита для канонического encoding композитного ключа инстанса. Используется для словарного
/// hash-key в <see cref="InstanceManager"/> (через <see cref="InstanceIdentity.EncodedKey"/>),
/// в <see cref="KeyspaceRegistry"/>-bucket-key и при lookup инстанса по <c>(stage, keys)</c>
/// без построения <see cref="InstanceIdentity"/>-объекта.
/// <para>
/// Human-readable форматирование (<c>FullyQualifiedName</c>) намеренно НЕ здесь — оно живёт
/// как приватная деталь <see cref="InstanceIdentity"/>, чтобы Identity owned all forms of itself
/// и никто снаружи не мог построить FQN по своим правилам.
/// </para>
/// </summary>
internal static class DependencyKey {
	/// <summary>
	/// Канонический encoding: компоненты отсортированы по имени; спецсимволы (<c>|</c>, <c>=</c>, <c>\</c>) экранируются
	/// обратным слэшем, чтобы исключить collision-возможность вида <c>{a: "1|b=2"}</c> vs <c>{a: "1", b: "2"}</c>.
	/// </summary>
	public static string Encode(IReadOnlyDictionary<string, string> keys) {
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
}
