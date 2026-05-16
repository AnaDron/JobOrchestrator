namespace JobOrchestrator.Tests.Abstractions;

/// <summary>
/// Проверка тестируемости <see cref="JobContext"/> с пользовательским <see cref="IJobContextSink"/>:
/// в этой проверке мы не используем <c>InternalsVisibleTo</c>, а конструируем JobContext через публичный
/// object-initializer с required-полями. Это симулирует unit-тест пользовательского <see cref="IJobService"/>.
/// </summary>
public sealed class JobContextSinkTests {
	private sealed class RecordingSink : IJobContextSink {
		public List<string> Added { get; } = [];
		public List<string> Removed { get; } = [];
		public ValueTask AddKeyAsync(string key, CancellationToken ct = default) {
			Added.Add(key);
			return ValueTask.CompletedTask;
		}
		public ValueTask RemoveKeyAsync(string key, CancellationToken ct = default) {
			Removed.Add(key);
			return ValueTask.CompletedTask;
		}
	}

	private sealed class NoopState : IJobState {
		public Task<T?> GetAsync<T>(string key, CancellationToken ct = default) => Task.FromResult<T?>(default);
		public Task SetAsync<T>(string key, T value, CancellationToken ct = default) => Task.CompletedTask;
		public Task RemoveAsync(string key, CancellationToken ct = default) => Task.CompletedTask;
	}

	[Fact]
	public async Task AddKey_DelegatesToSink() {
		var sink = new RecordingSink();
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Manual,
			State = new NoopState(),
			StageName = "x",
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = sink,
		};

		await ctx.AddKeyAsync("k1");
		await ctx.AddKeyAsync("k2");

		sink.Added.Should().Equal("k1", "k2");
		sink.Removed.Should().BeEmpty();
	}

	[Fact]
	public async Task RemoveKey_DelegatesToSink() {
		var sink = new RecordingSink();
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Auto,
			State = new NoopState(),
			StageName = "x",
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = sink,
		};

		await ctx.RemoveKeyAsync("a");
		sink.Removed.Should().Equal("a");
	}

	[Fact]
	public async Task AddKey_NullOrEmpty_Throws() {
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Auto,
			State = new NoopState(),
			StageName = "x",
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = new RecordingSink(),
		};

		Func<Task> addNull = async () => await ctx.AddKeyAsync(null!);
		Func<Task> addEmpty = async () => await ctx.AddKeyAsync("");
		await addNull.Should().ThrowAsync<ArgumentException>();
		await addEmpty.Should().ThrowAsync<ArgumentException>();
	}
}
