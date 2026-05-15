using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace JobOrchestrator.IntegrationTests.Support;

/// <summary>
/// Записывает каждый вызов <c>ExecuteAsync</c> со snapshot <see cref="JobContext"/>. Один recorder
/// inject-ится во все stage-services теста — централизует учёт исполнений, заменяя per-stage
/// <c>FakeServiceA.CallCount</c>+<c>Calls</c>-pattern.
/// <para>
/// Query API: <see cref="Count(string)"/> / <see cref="Count(string, IReadOnlyDictionary{string,string})"/>
/// / <see cref="ForStage"/>.
/// </para>
/// </summary>
internal sealed class ExecutionRecorder {
	private readonly ConcurrentQueue<Execution> _runs = new();

	public IReadOnlyList<Execution> Snapshot() => _runs.ToArray();

	public IReadOnlyList<Execution> ForStage(string stageName) =>
		_runs.Where(r => r.StageName == stageName).ToArray();

	public int Count(string stageName) =>
		_runs.Count(r => r.StageName == stageName);

	public int Count(string stageName, IReadOnlyDictionary<string, string> keys) =>
		_runs.Count(r => r.StageName == stageName && KeysEqual(r.DependencyKeys, keys));

	public void Record(JobContext ctx) {
		_runs.Enqueue(new Execution(
			ctx.StageName,
			ctx.FullyQualifiedName,
			ctx.DependencyKeys.ToImmutableDictionary(StringComparer.Ordinal),
			DateTimeOffset.UtcNow));
	}

	private static bool KeysEqual(IReadOnlyDictionary<string, string> a, IReadOnlyDictionary<string, string> b) {
		if (a.Count != b.Count) return false;
		foreach (var (k, v) in a) {
			if (!b.TryGetValue(k, out var bv) || bv != v) return false;
		}
		return true;
	}
}

internal sealed record Execution(
	string StageName,
	string FullyQualifiedName,
	IReadOnlyDictionary<string, string> DependencyKeys,
	DateTimeOffset At
);
