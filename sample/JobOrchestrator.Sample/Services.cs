using JobOrchestrator.Abstractions;
using Microsoft.Extensions.Logging;

namespace JobOrchestrator.Sample;

internal sealed class ProducerService(ILogger<ProducerService> logger) : IJobService {
	public async Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		logger.LogInformation("Producer iteration ({Instance})", ctx.FullyQualifiedName);
		await ctx.AddKeyAsync("alpha", ct);
		await ctx.AddKeyAsync("beta", ct);
		await ctx.AddKeyAsync("gamma", ct);
	}
}

internal sealed class ConsumerService(ILogger<ConsumerService> logger) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		var key = ctx.DependencyKeys["producer"];
		logger.LogInformation("Consumer iteration ({Instance}, key={Key})", ctx.FullyQualifiedName, key);
		return Task.CompletedTask;
	}
}

internal sealed class ReporterService(ILogger<ReporterService> logger) : IJobService {
	public Task ExecuteAsync(JobContext ctx, CancellationToken ct) {
		logger.LogInformation("Reporter iteration ({Instance})", ctx.FullyQualifiedName);
		return Task.CompletedTask;
	}
}
