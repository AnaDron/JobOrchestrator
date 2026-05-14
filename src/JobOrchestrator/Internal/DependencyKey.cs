using System.Text;

namespace JobOrchestrator.Internal;

/// <summary>
/// Утилиты работы с композитным ключом инстанса:
/// <list type="bullet">
/// <item><see cref="Encode"/> — канонический encoding для использования как ключ <c>Dictionary</c> в <see cref="InstanceManager"/> (sort by name).</item>
/// <item><see cref="FormatFullyQualifiedName"/> — человеко-читаемый идентификатор для логирования (порядок объявления зависимостей).</item>
/// </list>
/// </summary>
internal static class DependencyKey {
	/// <summary>
	/// Канонический encoding: компоненты отсортированы по имени; спецсимволы (<c>|</c>, <c>=</c>, <c>\</c>) экранируются
	/// обратным слэшем, чтобы исключить collision-возможность вида <c>{a: "1|b=2"}</c> vs <c>{a: "1", b: "2"}</c>.
	/// Используется ТОЛЬКО для словарного hash-ключа в <see cref="InstanceManager"/>, не для логов.
	/// Для логов — <see cref="FormatFullyQualifiedName"/>.
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

	/// <summary>
	/// FullyQualifiedName: <c>"stage[dep1=v1,dep2=v2,...]"</c> без пробелов после запятых.
	/// Порядок компонентов — <paramref name="orderedKeyNames"/> (заранее вычислен <c>StageRegistry.ExpectedKeyNames</c>
	/// с учётом транзитивного наследования через <c>DependsOn</c>-цепочки). Это даёт детерминированный
	/// FQN независимо от того, в каком порядке прилетали разрешающие события и в какой порядок hash-table
	/// уложила ключи в <c>ImmutableDictionary</c>.
	/// </summary>
	public static string FormatFullyQualifiedName(string stageName, IReadOnlyDictionary<string, string> keys, IReadOnlyList<string> orderedKeyNames) {
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
}
