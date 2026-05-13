using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Tests.Configuration;

/// <summary>
/// Проверка реального резолва стадий из IServiceProvider на старте: отсутствие регистрации,
/// нарушенный constructor, тип-не-реализующий-IJobService.
/// </summary>
public sealed class ValidateServiceRegistrationsTests {
	private sealed class GoodService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private sealed class NotAJobService { }

	private sealed class NeedsMissingDep(string required) : IJobService {
		public string Required => required;    // suppress IDE warning
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static StageDescriptor MakeStage(string name, Type serviceType) => new() {
		Name = name,
		ServiceType = serviceType,
		Interval = TimeSpan.FromMinutes(1),
		RetryPolicy = RetryPolicy.NoRetry,
		Debounce = TimeSpan.Zero,
		Dependencies = [],
	};

	[Fact]
	public void Validate_AllServicesProperlyRegistered_DoesNotThrow() {
		var registry = new StageRegistry([MakeStage("a", typeof(GoodService))]);
		var services = new ServiceCollection();
		services.AddScoped<GoodService>();
		using var provider = services.BuildServiceProvider();

		Action act = () => registry.ValidateServiceRegistrations(provider);
		act.Should().NotThrow();
	}

	[Fact]
	public void Validate_ServiceTypeNotRegistered_ThrowsConfigurationException() {
		var registry = new StageRegistry([MakeStage("a", typeof(GoodService))]);
		var services = new ServiceCollection();
		// GoodService НЕ зарегистрирован.
		using var provider = services.BuildServiceProvider();

		Action act = () => registry.ValidateServiceRegistrations(provider);
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*не зарегистрирован*");
	}

	[Fact]
	public void Validate_ServiceTypeNotImplementingIJobService_ThrowsConfigurationException() {
		// Стадия объявлена с типом, который НЕ реализует IJobService.
		var registry = new StageRegistry([MakeStage("a", typeof(NotAJobService))]);
		var services = new ServiceCollection();
		services.AddScoped<NotAJobService>();
		using var provider = services.BuildServiceProvider();

		Action act = () => registry.ValidateServiceRegistrations(provider);
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*не реализует IJobService*");
	}

	[Fact]
	public void Validate_ServiceConstructorParamMissing_ThrowsConfigurationException() {
		var registry = new StageRegistry([MakeStage("a", typeof(NeedsMissingDep))]);
		var services = new ServiceCollection();
		services.AddScoped<NeedsMissingDep>();
		// string-параметр конструктора НЕ зарегистрирован — DI должен бросить.
		using var provider = services.BuildServiceProvider();

		Action act = () => registry.ValidateServiceRegistrations(provider);
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*резолв*");
	}
}
