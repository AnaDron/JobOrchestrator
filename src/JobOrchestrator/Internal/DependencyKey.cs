using System.Text;

namespace JobOrchestrator.Internal;

/// <summary>
/// Утилиты работы с композитным ключом инстанса:
/// <list type="bullet">
/// <item><see cref="Encode"/> — канонический encoding для использования как ключ <c>Dictionary</c> в <see cref="JobManager"/> (sort by name).</item>
/// <item><see cref="FormatFullyQualifiedName"/> — человеко-читаемый идентификатор для логирования (порядок объявления зависимостей).</item>
/// </list>
/// </summary>
internal static class DependencyKey {
	/// <summary>
	/// Канонический encoding: компоненты отсортированы по имени для словарного hash-совпадения
	/// независимо от порядка вставки. Используется ТОЛЬКО для JobManager-словарного ключа,
	/// не для логов. Для логов — <see cref="FormatFullyQualifiedName"/>.
	/// </summary>
	public static string Encode(IReadOnlyDictionary<string, string> keys) {
		if (keys.Count == 0) return string.Empty;
		var sorted = keys.OrderBy(kv => kv.Key, StringComparer.Ordinal);
		StringBuilder sb = new();
		bool first = true;
		foreach (var kv in sorted) {
			if (!first) sb.Append('|');
			sb.Append(kv.Key).Append('=').Append(kv.Value);
			first = false;
		}
		return sb.ToString();
	}

	/// <summary>
	/// FullyQualifiedName: <c>"stage[dep1=v1,dep2=v2,...]"</c> без пробелов после запятых.
	/// Порядок компонентов — порядок их появления в <paramref name="keys"/> (insertion order).
	/// SDK заполняет <c>keys</c> в порядке объявления зависимостей в Fluent API при создании инстанса.
	/// Для безключевой стадии (пустой словарь) — <c>"stage[]"</c>.
	/// </summary>
	public static string FormatFullyQualifiedName(string stageName, IReadOnlyDictionary<string, string> keys) {
		if (keys.Count == 0) return $"{stageName}[]";
		StringBuilder sb = new(stageName.Length + keys.Count * 16);
		sb.Append(stageName).Append('[');
		bool first = true;
		foreach (var kv in keys) {
			if (!first) sb.Append(',');
			sb.Append(kv.Key).Append('=').Append(kv.Value);
			first = false;
		}
		sb.Append(']');
		return sb.ToString();
	}
}
