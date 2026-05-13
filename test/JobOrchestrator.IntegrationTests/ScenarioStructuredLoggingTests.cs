using JobOrchestrator.IntegrationTests.Support;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Проверяет, что <see cref="StageRunner"/> разворачивает <see cref="ILogger.BeginScope"/>
/// со структурными полями <c>FullyQualifiedName</c>, <c>StageName</c>, <c>CorrelationId</c>
/// и <c>{depStageName}Key</c> per компонент композитного ключа.
/// </summary>
public sealed class ScenarioStructuredLoggingTests {
	private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

	[Fact]
	public async Task BeginScope_ContainsStructuredFields() {
		var capturing = new CapturingLoggerProvider();

		var builder = Host.CreateApplicationBuilder();
		builder.Logging.ClearProviders();
		builder.Logging.AddProvider(capturing);

		builder.Services.AddInMemoryJobStateStore();
		builder.Services.AddJobOrchestrator(jobs => {
			var shops = jobs.Stage("shops")
				.HandledBy<FakeServiceA>()
				.RunPeriodically(TimeSpan.FromMinutes(1));
			jobs.Stage("productGroups")
				.HandledBy<FakeServiceB>()
				.DependsOnInstance(shops)
				.RunPeriodically(TimeSpan.FromMinutes(1));
		});
		builder.Services.AddSingleton<FakeServiceA>();
		builder.Services.AddSingleton<FakeServiceB>();
		builder.Services.AddSingleton<FakeServiceA>(sp => {
			var s = new FakeServiceA();
			s.ExecuteHandler = (ctx, _) => { ctx.AddKey("uuid-1"); return Task.CompletedTask; };
			return s;
		});

		using var host = builder.Build();
		var shopsFake = host.Services.GetRequiredService<FakeServiceA>();
		var pgFake = host.Services.GetRequiredService<FakeServiceB>();

		await host.StartAsync().ConfigureAwait(false);
		try {
			(await pgFake.WaitForCallCountAsync(1, Timeout).ConfigureAwait(false)).Should().BeTrue();
		} finally {
			await host.StopAsync().ConfigureAwait(false);
		}

		var pgScope = capturing.Scopes
			.FirstOrDefault(s =>
				s.TryGetValue("StageName", out var name) && string.Equals(name as string, "productGroups", StringComparison.Ordinal));
		pgScope.Should().NotBeNull("StageRunner должен разворачивать scope с FullyQualifiedName/StageName/depKey-полями.");
		pgScope!["FullyQualifiedName"].Should().Be("productGroups[shops=uuid-1]");
		pgScope.Should().ContainKey("CorrelationId");
		pgScope.Should().ContainKey("shopsKey");
		pgScope["shopsKey"].Should().Be("uuid-1");
	}
}
