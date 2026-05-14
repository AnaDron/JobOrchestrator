namespace JobOrchestrator.Tests.Helpers;

/// <summary>
/// Утилита для unit-тестов: создаёт fully-initialized <see cref="StageDescriptor"/> без поднятия
/// всего pipeline (<c>JobOrchestratorBuilder.BuildRegistry</c>). Использует фейковый
/// <see cref="IStageInitializer"/>, который для target-стадии возвращает указанные deps и
/// транзитивно-вычисленные <c>ExpectedKeyNames</c>; reverse-поля — empty, rank — 0.
/// <para>
/// Тесты, которым нужны конкретные reverse-поля (cascade-сценарии и т.п.), должны идти через
/// полный pipeline или собирать кастомный <see cref="IStageInitializer"/>.
/// </para>
/// </summary>
internal static class TestStages {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	/// <summary>
	/// Опции для построения тестового дескриптора. Все поля имеют разумные дефолты — задавай только
	/// то, что важно для конкретного теста.
	/// </summary>
	public sealed record Options {
		public TimeSpan Interval { get; init; } = TimeSpan.FromMinutes(1);
		public TimeSpan Debounce { get; init; } = TimeSpan.Zero;
		public RetryPolicy RetryPolicy { get; init; } = RetryPolicy.NoRetry;
		public TimeSpan? ExecutionTimeout { get; init; }
		public int? ConcurrencyLimit { get; init; }
		public Type ServiceType { get; init; } = typeof(FakeService);
		public IReadOnlyList<StageDependency> Dependencies { get; init; } = [];

		/// <summary>
		/// Если <c>null</c> — <see cref="StageDescriptor.ExpectedKeyNames"/> вычисляется транзитивно
		/// из <see cref="Dependencies"/> (production-like). Если задан явно — используется как-есть
		/// (для FQN-ordering-тестов с произвольным порядком имён).
		/// </summary>
		public IReadOnlyList<string>? ExpectedKeyNames { get; init; }
	}

	/// <summary>Builds fully-initialized StageDescriptor with optional configuration.</summary>
	public static StageDescriptor Make(string name, Options? options = null) {
		options ??= new Options();
		var stage = new StageDescriptor {
			Name = name,
			ServiceType = options.ServiceType,
			Interval = options.Interval,
			RetryPolicy = options.RetryPolicy,
			Debounce = options.Debounce,
			ExecutionTimeout = options.ExecutionTimeout,
			ConcurrencyLimit = options.ConcurrencyLimit,
		};
		var expected = options.ExpectedKeyNames ?? ComputeExpectedFromDeps(options.Dependencies);
		stage.Initialize(new FakeInitializer(stage, options.Dependencies, expected));
		return stage;
	}

	private static IReadOnlyList<string> ComputeExpectedFromDeps(IReadOnlyList<StageDependency> deps) {
		var names = new List<string>();
		var seen = new HashSet<string>(StringComparer.Ordinal);
		foreach (var dep in deps) {
			foreach (var inherited in dep.Target.ExpectedKeyNames) {
				if (seen.Add(inherited)) names.Add(inherited);
			}
			if (dep.Mode == DependencyMode.Instance && seen.Add(dep.Target.Name)) {
				names.Add(dep.Target.Name);
			}
		}
		return names;
	}

	/// <summary>
	/// Тестовый <see cref="IStageInitializer"/>: возвращает заданные значения для target-стадии,
	/// дефолтные empty/0 для всех остальных (на случай accidental пересечения).
	/// </summary>
	private sealed class FakeInitializer(
		StageDescriptor target,
		IReadOnlyList<StageDependency> deps,
		IReadOnlyList<string> expected
	) : IStageInitializer {
		public IReadOnlyList<StageDependency> DependenciesOf(StageDescriptor s) =>
			ReferenceEquals(s, target) ? deps : [];

		public IReadOnlyList<string> ExpectedKeyNamesOf(StageDescriptor s) =>
			ReferenceEquals(s, target) ? expected : [];

		public IReadOnlyList<StageDescriptor> DependentsWholeOf(StageDescriptor s) => [];
		public IReadOnlyList<StageDescriptor> DependentsInstanceOf(StageDescriptor s) => [];
		public IReadOnlyList<StageDescriptor> AffectedByKeyRemovalOf(StageDescriptor s) => [];
		public int CancellationRankOf(StageDescriptor s) => 0;
	}
}
