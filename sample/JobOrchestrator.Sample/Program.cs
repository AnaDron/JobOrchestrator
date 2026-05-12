using JobOrchestrator.Abstractions;
using JobOrchestrator.Configuration;
using JobOrchestrator.Hosting;
using JobOrchestrator.InMemory;
using JobOrchestrator.Sample;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Минимальный пример: producer-стадия эмитит три ключа, consumer-стадия параметризуется ими,
// reporter-стадия наследует ключи consumer через DependsOn. Запускается ~5 секунд, потом выходит.

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddSimpleConsole(o => {
	o.SingleLine = true;
	o.IncludeScopes = true;
	o.TimestampFormat = "HH:mm:ss ";
});

builder.Services.AddInMemoryJobStateStore();
builder.Services.AddJobOrchestrator(jobs => {
	var producer = jobs.Stage("producer")
		.HandledBy<ProducerService>()
		.RunPeriodically(TimeSpan.FromSeconds(10));

	var consumer = jobs.Stage("consumer")
		.HandledBy<ConsumerService>()
		.DependsOnInstance(producer)
		.RunPeriodically(TimeSpan.FromSeconds(2));

	jobs.Stage("reporter")
		.HandledBy<ReporterService>()
		.DependsOn(consumer)
		.RunPeriodically(TimeSpan.FromSeconds(3));
});

using var host = builder.Build();
await host.StartAsync();
await Task.Delay(TimeSpan.FromSeconds(5));
await host.StopAsync();
