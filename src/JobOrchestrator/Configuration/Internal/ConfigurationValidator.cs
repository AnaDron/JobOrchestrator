using JobOrchestrator.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Configuration.Internal;

/// <summary>
/// Все валидации Configuration-слоя в одном месте. Вызывается до построения дескрипторов и при
/// старте host'а: если что-то невалидно — бросает <see cref="JobConfigurationException"/> с
/// конкретным сообщением.
/// <para>
/// Проверки разделены по времени вызова:
/// </para>
/// <list type="bullet">
/// <item><see cref="Validate"/> (build-time, в <c>JobOrchestratorBuilder.BuildRegistry</c>):
/// per-stage конфиг, висячие зависимости, циклы.</item>
/// <item><see cref="ValidateServiceRegistrations"/> (start-time, в
/// <c>JobOrchestratorHostedService.StartAsync</c>): резолв <see cref="IJobService"/>-сервисов
/// из DI — отделено, потому что требует уже-построенный <see cref="IServiceProvider"/>.</item>
/// </list>
/// <para>
/// Дубли имён в build-time не проверяются — невозможны по конструкции
/// <see cref="JobOrchestratorBuilder.Stage"/> (inline TryAdd по Dictionary). Defense-in-depth
/// остаётся на уровне <see cref="StageRegistry"/>-ctor.
/// </para>
/// </summary>
internal static class ConfigurationValidator {
	/// <summary>Полная валидация со стороны pipeline: per-stage конфиг + structural.</summary>
	public static void Validate(IReadOnlyList<StageBuilder> builders) {
		ArgumentNullException.ThrowIfNull(builders);
		foreach (var sb in builders) {
			ValidateStageConfig(sb);
		}
		var rawDeps = builders.ToDictionary(x => x.Name, x => x.Dependencies, StringComparer.Ordinal);
		ValidateGraphStructure(rawDeps);
	}

	/// <summary>
	/// Structural-only: висячие зависимости + циклы. Принимает raw-deps-карту, чтобы быть
	/// тестируемой без построения <see cref="StageBuilder"/>-объектов (которые не позволяют
	/// объявить «dangling»-зависимости через Fluent API).
	/// </summary>
	public static void ValidateGraphStructure(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		ArgumentNullException.ThrowIfNull(rawDeps);
		ValidateNoDanglingDependencies(rawDeps);
		ValidateNoCycles(rawDeps);
	}

	/// <summary>
	/// Start-time валидация: для каждой стадии проверяет, что её <see cref="StageDescriptor.ServiceType"/>
	/// (a) зарегистрирован в DI, (b) реально резолвится (constructor-params не ломают граф),
	/// (c) реализует <see cref="IJobService"/>. Бросает <see cref="JobConfigurationException"/> с описанием
	/// первой найденной проблемы. Вызывается из <see cref="Hosting.JobOrchestratorHostedService.StartAsync"/>.
	/// </summary>
	public static void ValidateServiceRegistrations(
		IReadOnlyCollection<StageDescriptor> stages,
		IServiceProvider services
	) {
		ArgumentNullException.ThrowIfNull(stages);
		ArgumentNullException.ThrowIfNull(services);
		using var scope = services.CreateScope();
		foreach (var stage in stages) {
			object? resolved;
			try {
				resolved = scope.ServiceProvider.GetService(stage.ServiceType);
			} catch (Exception ex) {
				throw new JobConfigurationException(
					$"Стадия '{stage.Name}': резолв {stage.ServiceType.FullName} из DI завершился с ошибкой: {ex.Message}", ex);
			}
			if (resolved is null) {
				throw new JobConfigurationException(
					$"Стадия '{stage.Name}': тип {stage.ServiceType.FullName} не зарегистрирован в DI.");
			}
			if (resolved is not IJobService) {
				throw new JobConfigurationException(
					$"Стадия '{stage.Name}': тип {stage.ServiceType.FullName} зарегистрирован, но не реализует IJobService.");
			}
		}
	}

	private static void ValidateStageConfig(StageBuilder sb) {
		if (sb.ServiceType is null) {
			throw new JobConfigurationException($"Стадия '{sb.Name}': HandledBy<TService>() не задано.");
		}
		if (sb.Interval <= TimeSpan.Zero) {
			throw new JobConfigurationException($"Стадия '{sb.Name}': RunPeriodically(...) не задано.");
		}
	}

	private static void ValidateNoDanglingDependencies(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		foreach (var kv in rawDeps) {
			foreach (var dep in kv.Value) {
				if (!rawDeps.ContainsKey(dep.TargetName)) {
					throw new JobConfigurationException(
						$"Стадия '{kv.Key}' зависит от несуществующей стадии '{dep.TargetName}'.");
				}
			}
		}
	}

	private static void ValidateNoCycles(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		HashSet<string> gray = new(StringComparer.Ordinal);
		HashSet<string> black = new(StringComparer.Ordinal);
		foreach (var name in rawDeps.Keys) {
			DfsCheck(name, rawDeps, gray, black, []);
		}
	}

	private static void DfsCheck(
		string node,
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps,
		HashSet<string> gray,
		HashSet<string> black,
		List<string> path
	) {
		if (black.Contains(node)) return;
		if (!gray.Add(node)) {
			int cycleStart = path.IndexOf(node);
			List<string> cycle = [.. path[cycleStart..], node];
			throw new JobConfigurationException($"Цикл в графе зависимостей: {string.Join(" -> ", cycle)}");
		}
		path.Add(node);
		if (rawDeps.TryGetValue(node, out var deps)) {
			foreach (var dep in deps) {
				DfsCheck(dep.TargetName, rawDeps, gray, black, path);
			}
		}
		path.RemoveAt(path.Count - 1);
		gray.Remove(node);
		black.Add(node);
	}
}
