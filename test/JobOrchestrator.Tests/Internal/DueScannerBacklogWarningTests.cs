using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Проверка ChannelBacklog warning в <see cref="DueScanner"/>: при достижении порога 7000 в очереди
/// должна сработать одна запись на интервал suppression (30 сек), повторные срабатывания —
/// только после истечения интервала.
/// </summary>
public sealed class DueScannerBacklogWarningTests {
	private sealed class TestLogger : ILogger {
		public List<(LogLevel Level, EventId EventId, string Message)> Records { get; } = [];

		public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
		public bool IsEnabled(LogLevel logLevel) => true;
		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) {
			Records.Add((logLevel, eventId, formatter(state, exception)));
		}

		private sealed class NullScope : IDisposable {
			public static readonly NullScope Instance = new();
			public void Dispose() { }
		}
	}

	private sealed class FakeLogger<T>(TestLogger inner) : ILogger<T> {
		public IDisposable BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);
		public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);
		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
			=> inner.Log(logLevel, eventId, state, exception, formatter);
	}

	private const int BacklogWarningEventId = 5003;

	[Fact]
	public void WarnOnChannelBacklog_BelowThreshold_DoesNotLog() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		// Заполним только 100 элементов — гораздо ниже 7000.
		for (int i = 0; i < 100; i++) channel.Writer.TryWrite(CreateDummyEvent());

		var capture = new TestLogger();
		var scanner = new DueScanner(new InstanceManager(), channel, TimeProvider.System, new FakeLogger<DueScanner>(capture));
		scanner.WarnOnChannelBacklog(DateTimeOffset.UtcNow);

		capture.Records.Should().NotContain(r => r.EventId.Id == BacklogWarningEventId);
		scanner.Dispose();
	}

	[Fact]
	public void WarnOnChannelBacklog_AboveThreshold_LogsOnceThenSuppresses() {
		var channel = Channel.CreateUnbounded<OrchestratorEvent>();
		// 7500 — выше порога 7000.
		for (int i = 0; i < 7500; i++) channel.Writer.TryWrite(CreateDummyEvent());

		var capture = new TestLogger();
		var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
		var scanner = new DueScanner(new InstanceManager(), channel, fakeTime, new FakeLogger<DueScanner>(capture));

		scanner.WarnOnChannelBacklog(fakeTime.GetUtcNow());
		// Сразу — повтор, suppression-окно ещё не истекло.
		scanner.WarnOnChannelBacklog(fakeTime.GetUtcNow().AddSeconds(5));
		scanner.WarnOnChannelBacklog(fakeTime.GetUtcNow().AddSeconds(29));

		capture.Records.Count(r => r.EventId.Id == BacklogWarningEventId)
			.Should().Be(1, "три вызова в пределах suppression-окна → одна запись");

		// За suppression-окно (>30с) — следующий вызов снова логирует.
		scanner.WarnOnChannelBacklog(fakeTime.GetUtcNow().AddSeconds(31));
		capture.Records.Count(r => r.EventId.Id == BacklogWarningEventId)
			.Should().Be(2, "после истечения suppression-окна — повторный warning");
		scanner.Dispose();
	}

	private static OrchestratorEvent CreateDummyEvent() {
		// Все события orchestrator-loop'а привязаны к StageInstance. Создаём минимально валидный
		// инстанс stub-стадии и используем StageCompletedEvent (без побочных эффектов в Channel.Reader).
		var stage = new StageDescriptor {
			Name = "stub",
			ServiceType = typeof(object),
			Interval = TimeSpan.FromMinutes(1),
			RetryPolicy = RetryPolicy.NoRetry,
			Debounce = TimeSpan.Zero,
			Dependencies = [],
		};
		var inst = new StageInstance {
			Stage = stage,
			DependencyKeys = new Dictionary<string, string>(StringComparer.Ordinal),
			FullyQualifiedName = "stub[]",
			EncodedKey = "",
		};
		return new StageCompletedEvent(inst, DateTimeOffset.UtcNow);
	}
}
