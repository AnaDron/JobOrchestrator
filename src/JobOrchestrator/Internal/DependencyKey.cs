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
		return string.Join("|", keys.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}"));
	}

	/// <summary>
	/// FullyQualifiedName: <c>"stage[dep1=v1,dep2=v2,...]"</c> без пробелов после запятых.
	/// Порядок компонентов — порядок их появления в <paramref name="keys"/> (insertion order).
	/// SDK заполняет <c>keys</c> в порядке объявления зависимостей в Fluent API при создании инстанса.
	/// Для безключевой стадии (пустой словарь) — <c>"stage[]"</c>.
	/// </summary>
	public static string FormatFullyQualifiedName(string stageName, IReadOnlyDictionary<string, string> keys) {
		if (keys.Count == 0) return $"{stageName}[]";
		return $"{stageName}[{string.Join(",", keys.Select(kv => $"{kv.Key}={kv.Value}"))}]";
	}
}
