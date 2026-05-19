using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace JobOrchestrator.Hosting;

/// <summary>Builder-extensions для конфигурации host-уровневых параметров оркестратора.</summary>
public static class JobOrchestratorBuilderHostExtensions {
	/// <summary>
	/// Настраивает <see cref="JobOrchestratorHostOptions"/> (fault-threshold handler'ов,
	/// shutdown-timeout итераций) для текущего <see cref="JobOrchestratorBuilder"/>. Форма регистрации
	/// выбирается из <see cref="JobOrchestratorBuilder.TenantKey"/>: для одиночного orchestrator-а
	/// (TenantKey=null) — non-keyed singleton, для tenant'ового — keyed singleton под этим же ключом.
	/// <para>
	/// Каждый tenant получает физически независимый экземпляр <see cref="JobOrchestratorHostOptions"/>
	/// (резолвится через <see cref="Keyed.KeyedAwareServiceProvider"/>), что позволяет задать разные
	/// crash-tolerance / shutdown-timeout профили на разных tenant'ах без cross-contamination.
	/// </para>
	/// <para>
	/// Семантика повтора — first-wins (<c>TryAdd*</c>). Повторный вызов <see cref="ConfigureJobOrchestratorHost"/>
	/// в рамках одного builder'а (например, при multi-call accumulation того же tenant'а из разных модулей) —
	/// no-op. На lazy-pass (<see cref="JobOrchestratorBuilder.Services"/> равен <c>null</c>) метод тоже no-op:
	/// регистрация уже выполнена на probe-pass.
	/// </para>
	/// </summary>
	public static JobOrchestratorBuilder ConfigureJobOrchestratorHost(
		this JobOrchestratorBuilder builder,
		Action<JobOrchestratorHostOptions> configure
	) {
		ArgumentNullException.ThrowIfNull(builder);
		ArgumentNullException.ThrowIfNull(configure);
		if (builder.Services is null) return builder;
		var options = new JobOrchestratorHostOptions();
		configure(options);
		if (string.IsNullOrEmpty(builder.TenantKey)) {
			builder.Services.TryAddSingleton(options);
		} else {
			builder.Services.TryAddKeyedSingleton(builder.TenantKey, options);
		}
		return builder;
	}
}
