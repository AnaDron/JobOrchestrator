namespace JobOrchestrator.Abstractions;

/// <summary>
/// Публичные константы имён доменов оркестратора.
/// </summary>
public static class DomainName {
	/// <summary>
	/// Имя root-домена — пустая строка. Используется как ключ для <see cref="IJobOrchestrator.this[string]"/>
	/// при доступе к бездоменным стадиям; эквивалентно свойству <see cref="IJobOrchestrator.Root"/>.
	/// </summary>
	public const string Root = "";
}
