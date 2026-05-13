using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Тестовый <see cref="ILoggerProvider"/>, перехватывающий объекты, переданные в <see cref="ILogger.BeginScope"/>.
/// Позволяет проверять структурные поля scope-словарей, развёрнутых <see cref="StageRunner"/>.
/// </summary>
internal sealed class CapturingLoggerProvider : ILoggerProvider {
	public ConcurrentBag<IReadOnlyDictionary<string, object?>> Scopes { get; } = new();

	public ILogger CreateLogger(string categoryName) => new CapturingLogger(this);

	public void Dispose() { }

	private sealed class CapturingLogger(CapturingLoggerProvider parent) : ILogger {
		public IDisposable BeginScope<TState>(TState state) where TState : notnull {
			if (state is IEnumerable<KeyValuePair<string, object?>> kvs) {
				var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
				foreach (var kv in kvs) dict[kv.Key] = kv.Value;
				parent.Scopes.Add(dict);
			} else if (state is IEnumerable<KeyValuePair<string, object>> kvsNN) {
				var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
				foreach (var kv in kvsNN) dict[kv.Key] = kv.Value;
				parent.Scopes.Add(dict);
			}
			return NullScope.Instance;
		}

		public bool IsEnabled(LogLevel logLevel) => true;
		public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }

		private sealed class NullScope : IDisposable {
			public static readonly NullScope Instance = new();
			public void Dispose() { }
		}
	}
}
