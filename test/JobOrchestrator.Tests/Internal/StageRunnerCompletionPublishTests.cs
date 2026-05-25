using System.Threading.Channels;
using JobOrchestrator.Internal;
using JobOrchestrator.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Если completion-событие не попало в channel (shutdown / fault), <see cref="StageRunner"/>
/// снимает <c>Running</c> в <c>finally</c> — иначе флаг залипает до остановки процесса.
/// </summary>
public sealed class StageRunnerCompletionPublishTests {
	private sealed class NoOpJobService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private sealed class FailingJobService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) =>
			throw new InvalidOperationException("test failure");
	}

	private sealed class NullJobStateStore : IJobStateStore {
		public Task<string?> GetAsync(string scope, string key, CancellationToken ct) =>
			Task.FromResult<string?>(null);

		public Task SetAsync(string scope, string key, string value, CancellationToken ct) =>
			Task.CompletedTask;

		public Task RemoveAsync(string scope, string key, CancellationToken ct) =>
			Task.CompletedTask;

		public Task RemoveScopeAsync(string scope, CancellationToken ct) =>
			Task.CompletedTask;
	}

	private static StageRunner CreateRunner(
		Channel<OrchestratorEvent> channel,
		OrchestratorLifecycle lifecycle,
		IServiceProvider root
	) => new(
		root,
		root.GetRequiredService<IJobStateStore>(),
		channel,
		lifecycle,
		TimeProvider.System,
		NullLoggerFactory.Instance);

	[Fact]
	public async Task RunIterationAsync_WhenChannelClosedAfterSuccess_EndRunningClearsRunningFlag() {
		var stage = TestStages.Make("x", new() { ServiceType = typeof(NoOpJobService) });
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var lifecycle = new OrchestratorLifecycle(channel);

		var services = new ServiceCollection();
		services.AddSingleton<IJobStateStore, NullJobStateStore>();
		services.AddSingleton<NoOpJobService>();
		var root = services.BuildServiceProvider();

		var instance = new Instance { Identity = new InstanceIdentity(stage) };
		instance.Sink = new ChannelJobContextSink(channel.Writer, instance);
		instance.TryBeginRunning().Should().BeTrue();

		lifecycle.CloseChannel();

		var runner = CreateRunner(channel, lifecycle, root);
		await runner.RunIterationAsync(instance, TriggerSource.Auto, NoopRelease, CancellationToken.None).ConfigureAwait(false);

		instance.IsRunning.Should().BeFalse("completion не опубликован — runner обязан снять Running");
		channel.Reader.TryRead(out _).Should().BeFalse("закрытый channel не принимает StageCompleted");
	}

	[Fact]
	public async Task RunIterationAsync_WhenChannelClosedAfterFailure_EndRunningClearsRunningFlag() {
		var stage = TestStages.Make("x", new() { ServiceType = typeof(FailingJobService) });
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var lifecycle = new OrchestratorLifecycle(channel);

		var services = new ServiceCollection();
		services.AddSingleton<IJobStateStore, NullJobStateStore>();
		services.AddSingleton<FailingJobService>();
		var root = services.BuildServiceProvider();

		var instance = new Instance { Identity = new InstanceIdentity(stage) };
		instance.Sink = new ChannelJobContextSink(channel.Writer, instance);
		instance.TryBeginRunning().Should().BeTrue();

		lifecycle.CloseChannel();

		var runner = CreateRunner(channel, lifecycle, root);
		await runner.RunIterationAsync(instance, TriggerSource.Auto, NoopRelease, CancellationToken.None).ConfigureAwait(false);

		instance.IsRunning.Should().BeFalse("StageFailed не опубликован — runner обязан снять Running");
		channel.Reader.TryRead(out _).Should().BeFalse();
	}

	[Fact]
	public async Task RunIterationAsync_WhenCompletionPublished_LeavesRunningForEventLoop() {
		var stage = TestStages.Make("x", new() { ServiceType = typeof(NoOpJobService) });
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		var lifecycle = new OrchestratorLifecycle(channel);

		var services = new ServiceCollection();
		services.AddSingleton<IJobStateStore, NullJobStateStore>();
		services.AddSingleton<NoOpJobService>();
		var root = services.BuildServiceProvider();

		var instance = new Instance { Identity = new InstanceIdentity(stage) };
		instance.Sink = new ChannelJobContextSink(channel.Writer, instance);
		instance.TryBeginRunning().Should().BeTrue();

		var runner = CreateRunner(channel, lifecycle, root);
		await runner.RunIterationAsync(instance, TriggerSource.Auto, NoopRelease, CancellationToken.None).ConfigureAwait(false);

		instance.IsRunning.Should().BeTrue("при успешной публикации EndRunning делает только event loop");
		channel.Reader.TryRead(out var evt).Should().BeTrue();
		evt.Should().BeOfType<StageCompletedEvent>();
	}

	private static readonly Action NoopRelease = () => { };
}
