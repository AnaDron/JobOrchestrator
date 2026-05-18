using JobOrchestrator.Abstractions;
using JobOrchestrator.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace JobOrchestrator.InMemory;

/// <summary>Регистрация in-memory backend для <see cref="IJobStateStore"/> через builder.</summary>
public static class JobOrchestratorBuilderInMemoryExtensions {
	/// <summary>
	/// Регистрирует <see cref="InMemoryJobStateStore"/> как backend <see cref="IJobStateStore"/> для
	/// текущего <see cref="JobOrchestratorBuilder"/>. Форма регистрации выбирается из
	/// <see cref="JobOrchestratorBuilder.TenantKey"/>: для одиночного orchestrator-а (TenantKey=null)
	/// — non-keyed singleton, для tenant'ового — keyed singleton под этим же ключом.
	/// <para>
	/// Каждый tenant получает физически независимый экземпляр <see cref="InMemoryJobStateStore"/> —
	/// state-scope'ы разных tenant'ов не пересекаются на DI-уровне без необходимости в prefix-логике.
	/// </para>
	/// <para>
	/// На lazy-pass (повторный <c>configure</c>-вызов через
	/// <see cref="Hosting.IJobsConfigure.Apply"/> на DI-singleton builder'е) метод no-op:
	/// <see cref="JobOrchestratorBuilder.Services"/> равен <c>null</c>, регистрация уже выполнена
	/// на probe-pass.
	/// </para>
	/// </summary>
	public static JobOrchestratorBuilder UseInMemoryStateStore(this JobOrchestratorBuilder builder) {
		ArgumentNullException.ThrowIfNull(builder);
		if (builder.Services is null) return builder;
		if (string.IsNullOrEmpty(builder.TenantKey)) {
			builder.Services.TryAddSingleton<IJobStateStore, InMemoryJobStateStore>();
		} else {
			builder.Services.TryAddKeyedSingleton<IJobStateStore, InMemoryJobStateStore>(builder.TenantKey);
		}
		return builder;
	}
}
