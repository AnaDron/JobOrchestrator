using JobOrchestrator.Internal;

namespace JobOrchestrator.Configuration.Internal;

internal sealed class StageBuilder(string name, JobDefaults defaults) : IStageBuilder {
	private readonly List<StageDependency> _dependencies = [];

	public string Name { get; } = name;
	public Type? ServiceType { get; private set; }
	public TimeSpan Interval { get; private set; }
	// Snapshot defaults в момент создания стадии — последующие изменения JobDefaults не влияют.
	public RetryPolicy RetryPolicy { get; private set; } = defaults.RetryAfterFailure;
	public TimeSpan Debounce { get; private set; } = defaults.Debounce;
	public TimeSpan? ExecutionTimeout { get; private set; } = defaults.ExecutionTimeout;
	public int? ConcurrencyLimit { get; private set; }

	public IStageBuilder HandledBy<TService>() where TService : class, IJobService {
		ServiceType = typeof(TService);
		return this;
	}

	public IStageBuilder RunPeriodically(TimeSpan interval) {
		if (interval <= TimeSpan.Zero) {
			throw new ArgumentOutOfRangeException(nameof(interval), "Интервал должен быть положительным.");
		}
		Interval = interval;
		return this;
	}

	public IStageBuilder RetryAfterFailure(RetryPolicy policy) {
		ArgumentNullException.ThrowIfNull(policy);
		RetryPolicy = policy;
		return this;
	}

	IStageBuilder IStageBuilder.Debounce(TimeSpan window) {
		if (window < TimeSpan.Zero) {
			throw new ArgumentOutOfRangeException(nameof(window), "Окно дебаунса не может быть отрицательным.");
		}
		Debounce = window;
		return this;
	}

	public IStageBuilder WithExecutionTimeout(TimeSpan timeout) {
		if (timeout <= TimeSpan.Zero) {
			throw new ArgumentOutOfRangeException(nameof(timeout), "Watchdog-таймаут должен быть положительным.");
		}
		ExecutionTimeout = timeout;
		return this;
	}

	public IStageBuilder WithConcurrencyLimit(int maxParallel) {
		if (maxParallel <= 0) {
			throw new ArgumentOutOfRangeException(nameof(maxParallel), "Лимит параллельных итераций должен быть положительным.");
		}
		ConcurrencyLimit = maxParallel;
		return this;
	}

	public IStageBuilder DependsOn(IStageBuilder other) {
		AddDependency(other, DependencyMode.Whole);
		return this;
	}

	public IStageBuilder DependsOnInstance(IStageBuilder other) {
		AddDependency(other, DependencyMode.Instance);
		return this;
	}

	private void AddDependency(IStageBuilder other, DependencyMode mode) {
		ArgumentNullException.ThrowIfNull(other);
		if (other is not StageBuilder dep) {
			throw new JobConfigurationException($"DependsOn-цель '{other.Name}' создана не текущим JobOrchestratorBuilder.");
		}
		if (ReferenceEquals(dep, this)) {
			throw new JobConfigurationException($"Стадия '{Name}' не может зависеть от самой себя.");
		}
		// Сохраняем порядок объявления — IReadOnlyList в StageDescriptor.
		_dependencies.Add(new StageDependency(dep.Name, mode));
	}

	internal StageDescriptor BuildDescriptor() {
		if (ServiceType is null) {
			throw new JobConfigurationException($"Стадия '{Name}': HandledBy<TService>() не задано.");
		}
		if (Interval <= TimeSpan.Zero) {
			throw new JobConfigurationException($"Стадия '{Name}': RunPeriodically(...) не задано.");
		}
		var deps = _dependencies.ToArray();
		// Имена ожидаемых ключей вычисляются позже в StageRegistry — там доступны транзитивные
		// зависимости (имена, унаследованные через цепочку DependsOn-родителей).
		return new StageDescriptor {
			Name = Name,
			ServiceType = ServiceType,
			Interval = Interval,
			RetryPolicy = RetryPolicy,
			Debounce = Debounce,
			ExecutionTimeout = ExecutionTimeout,
			ConcurrencyLimit = ConcurrencyLimit,
			Dependencies = deps,
		};
	}
}
