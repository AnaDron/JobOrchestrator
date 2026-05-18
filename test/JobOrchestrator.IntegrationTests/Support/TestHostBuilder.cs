using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Factory-методы для canonical топологий тестов. Централизует DI-registration + Fluent API
/// конфигурацию для типовых сценариев — снижает boilerplate, увеличивает consistency между тестами.
/// </summary>
internal static class TestHostBuilder {
	/// <summary>
	/// Полная Evotor-топология: shops → productGroups → products → documents + employees.
	/// Используется в bootstrap-chain / key-propagation / structured-logging тестах.
	/// </summary>
	/// <param name="recorder">Recorder, записывающий все executions всех стадий.</param>
	/// <param name="shopsKeys">Источник batch-ей ключей для shops (типично EnqueueAdds("u-1", "u-2") перед StartAsync).</param>
	/// <param name="extra">Опциональная дополнительная DI-регистрация.</param>
	/// <param name="loggerProvider">Опциональный capturing-logger для structured logging-тестов.</param>
	/// <param name="interval">Интервал auto-tick для всех стадий (default = 80мс — баланс reactivity vs test speed).</param>
	public static IHost BuildEvotorGraph(
		ExecutionRecorder recorder,
		ShopsKeySource shopsKeys,
		Action<IServiceCollection>? extra = null,
		ILoggerProvider? loggerProvider = null,
		TimeSpan? interval = null
	) {
		var b = Host.CreateApplicationBuilder();
		b.Logging.ClearProviders();
		b.Logging.SetMinimumLevel(LogLevel.Trace);
		if (loggerProvider is not null) b.Logging.AddProvider(loggerProvider);

		var period = interval ?? TimeSpan.FromMilliseconds(80);
		b.Services.AddSingleton(recorder);
		b.Services.AddSingleton(shopsKeys);
		b.Services.AddScoped<ShopsStageService>();
		b.Services.AddScoped<ProductGroupsStageService>();
		b.Services.AddScoped<ProductsStageService>();
		b.Services.AddScoped<EmployeesStageService>();
		b.Services.AddScoped<DocumentsStageService>();
		extra?.Invoke(b.Services);

		b.Services.AddJobOrchestrator(jobs => {
			jobs.UseInMemoryStateStore();
			jobs.Defaults.Debounce = TimeSpan.FromMilliseconds(10);
			jobs.Defaults.RetryAfterFailure = RetryPolicy.FixedDelay(TimeSpan.FromMilliseconds(50));

			var shops = jobs.Stage("shops")
				.HandledBy<ShopsStageService>()
				.RunPeriodically(period);
			var employees = jobs.Stage("employees")
				.HandledBy<EmployeesStageService>()
				.DependsOn(shops)
				.RunPeriodically(period);
			var productGroups = jobs.Stage("productGroups")
				.HandledBy<ProductGroupsStageService>()
				.DependsOnInstance(shops)
				.RunPeriodically(period);
			var products = jobs.Stage("products")
				.HandledBy<ProductsStageService>()
				.DependsOn(productGroups)
				.RunPeriodically(period);
			jobs.Stage("documents")
				.HandledBy<DocumentsStageService>()
				.DependsOn(products)
				.DependsOn(employees)
				.RunPeriodically(period);
		});

		return b.Build();
	}
}
