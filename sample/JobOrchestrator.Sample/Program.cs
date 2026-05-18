using JobOrchestrator.Abstractions;
using JobOrchestrator.Configuration;
using JobOrchestrator.Hosting;
using JobOrchestrator.InMemory;
using JobOrchestrator.Sample;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Production-style пример: демонстрирует
//   • RegisterKey для bootstrap keyspace из «БД» (имитируется массивом);
//   • RetryAfterFailure + Debounce + WithExecutionTimeout + WithConcurrencyLimit;
//   • DependsOnInstance для multi-instance параметризации;
//   • Synchronous GetOverview() для post-mortem snapshot.

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddSimpleConsole(o => {
	o.SingleLine = true;
	o.IncludeScopes = true;
	o.TimestampFormat = "HH:mm:ss ";
});

builder.Services.AddJobOrchestrator(jobs => {
	jobs.UseInMemoryStateStore();
	jobs.Defaults = new JobDefaults {
		Debounce = TimeSpan.FromSeconds(1),
		RetryAfterFailure = RetryPolicy.ExponentialBackoff(
			initial: TimeSpan.FromSeconds(2),
			max: TimeSpan.FromMinutes(1)),
	};

	// producer: keyless, эмитит ключи внутри ExecuteAsync.
	var producer = jobs.Stage("producer")
		.HandledBy<ProducerService>()
		.RunPeriodically(TimeSpan.FromMinutes(5))
		.WithExecutionTimeout(TimeSpan.FromSeconds(30));    // watchdog

	// consumer: на каждый ключ producer'а — отдельный инстанс. Limit=2 — не более 2-х параллельно.
	var consumer = jobs.Stage("consumer")
		.HandledBy<ConsumerService>()
		.DependsOnInstance(producer)
		.RunPeriodically(TimeSpan.FromSeconds(10))
		.RetryAfterFailure(RetryPolicy.FixedDelay(TimeSpan.FromSeconds(5)))
		.WithConcurrencyLimit(2);

	// reporter: наследует ключи consumer'а через DependsOn (fan-out с inheritance).
	jobs.Stage("reporter")
		.HandledBy<ReporterService>()
		.DependsOn(consumer)
		.RunPeriodically(TimeSpan.FromSeconds(15));
});

using var host = builder.Build();
var orchestrator = host.Services.GetRequiredService<IJobOrchestrator>();

await host.StartAsync();

// Bootstrap keyspace из «БД» — типовой production-сценарий, когда оркестратор подхватывает
// набор объектов, который уже существует, и не ждёт первого producer-успеха для их обнаружения.
Console.WriteLine("\n=== Bootstrap: RegisterKey для существующих объектов ===");
var producer = orchestrator["producer"];
foreach (var shopId in new[] { "shop-100", "shop-200", "shop-300" }) {
	producer.RegisterKey(shopId);
}

await Task.Delay(TimeSpan.FromSeconds(5));

// Synchronous lock-free snapshot — доступен даже в IsFaulted (post-mortem-диагностика).
Console.WriteLine($"\n=== Snapshot (IsFaulted={orchestrator.IsFaulted}) ===");
var overview = orchestrator.GetOverview();
foreach (var info in overview.Instances.OrderBy(i => i.FullyQualifiedName, StringComparer.Ordinal)) {
	Console.WriteLine($"  {info.FullyQualifiedName,-50} state={info.State}  ok={info.LastSuccess:HH:mm:ss}  fails={info.ConsecutiveFailures}");
}
Console.WriteLine();

// Manual trigger конкретного инстанса — handle-API через индексатор.
Console.WriteLine("=== Manual trigger reporter[producer=shop-100] ===");
var result = await orchestrator["reporter"][("producer", "shop-100")].TriggerAsync();
Console.WriteLine($"  result = {result}");

await Task.Delay(TimeSpan.FromSeconds(3));
await host.StopAsync();
