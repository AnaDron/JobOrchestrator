namespace JobOrchestrator.Tests.Helpers;

/// <summary>
/// Тест-хелперы для работы с dependency-ключами. Содержит утилиты, которые не нужны в
/// production-коде, но полезны в тестах для проверки совместимости словарей ключей.
/// </summary>
internal static class DependencyHelpers {
	/// <summary>Проверка совместимости двух словарей: общие ключи должны иметь одинаковые значения.</summary>
	public static bool AreCompatible(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b) =>
		!a.Any(kv => b.TryGetValue(kv.Key, out var bv) && !string.Equals(bv, kv.Value, StringComparison.Ordinal));
}
