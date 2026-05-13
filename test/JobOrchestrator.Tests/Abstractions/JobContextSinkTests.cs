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
		public void AddKey(string key) => Added.Add(key);
		public void RemoveKey(string key) => Removed.Add(key);
	}

	private sealed class NoopState : IJobState {
		public Task<T?> GetAsync<T>(string key, CancellationToken ct = default) => Task.FromResult<T?>(default);
		public Task SetAsync<T>(string key, T value, CancellationToken ct = default) => Task.CompletedTask;
		public Task RemoveAsync(string key, CancellationToken ct = default) => Task.CompletedTask;
	}

	[Fact]
	public void AddKey_DelegatesToSink() {
		var sink = new RecordingSink();
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Manual,
			State = new NoopState(),
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = sink,
		};

		ctx.AddKey("k1");
		ctx.AddKey("k2");

		sink.Added.Should().Equal("k1", "k2");
		sink.Removed.Should().BeEmpty();
	}

	[Fact]
	public void RemoveKey_DelegatesToSink() {
		var sink = new RecordingSink();
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Auto,
			State = new NoopState(),
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = sink,
		};

		ctx.RemoveKey("a");
		sink.Removed.Should().Equal("a");
	}

	[Fact]
	public void AddKey_NullOrEmpty_Throws() {
		var ctx = new JobContext {
			CorrelationId = "c1",
			Trigger = TriggerSource.Auto,
			State = new NoopState(),
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "x[]",
			Sink = new RecordingSink(),
		};

		Action addNull = () => ctx.AddKey(null!);
		Action addEmpty = () => ctx.AddKey("");
		addNull.Should().Throw<ArgumentException>();
		addEmpty.Should().Throw<ArgumentException>();
	}
}
