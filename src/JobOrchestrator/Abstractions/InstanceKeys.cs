using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace JobOrchestrator.Abstractions;

/// <summary>
/// Композитный ключ инстанса стадии — иммутабельный набор пар (имя зависимости → значение ключа).
/// <para>
/// <c>readonly struct</c> с value-equality по каноническому encoded-string (детерминированно, ordinal-сортировка
/// по именам ключей, escape <c>\</c>, <c>|</c>, <c>=</c>). Создаётся через конструктор с params-Span либо через
/// implicit-конверсии:
/// </para>
/// <code>
/// stage[InstanceKeys.Empty]                  // keyless
/// stage[("shops", "u1")]                     // tuple → implicit
/// stage[new InstanceKeys(("k1","v1"), ("k2","v2"))]
/// </code>
/// <para>
/// <b>Boxing trade-off:</b> при cast к <see cref="IReadOnlyDictionary{TKey, TValue}"/> struct boxes в heap.
/// Handle-API возвращает <see cref="InstanceKeys"/> по value (без boxing); cast к BCL-интерфейсу —
/// явный opt-in caller'а. Используйте сам <see cref="InstanceKeys"/> там, где это допустимо.
/// </para>
/// <para>
/// <b>Default равен Empty:</b> <c>default(InstanceKeys)</c> семантически эквивалентен <see cref="Empty"/>
/// — оба представляют безключевой инстанс и равны по <c>Equals</c>/<c>GetHashCode</c>.
/// </para>
/// </summary>
public readonly struct InstanceKeys : IReadOnlyDictionary<string, string>, IEquatable<InstanceKeys> {
	private static readonly ImmutableDictionary<string, string> EmptyEntries =
		ImmutableDictionary<string, string>.Empty.WithComparers(StringComparer.Ordinal);

	private readonly ImmutableDictionary<string, string>? _entries;
	private readonly string? _encodedKey;

	/// <summary>Пустой ключ — для стадий без <c>DependsOnInstance</c>-зависимостей.</summary>
	public static InstanceKeys Empty { get; } = new InstanceKeys(EmptyEntries, string.Empty);

	/// <summary>
	/// Создаёт ключ из набора пар. На пустом наборе возвращает <see cref="Empty"/>-семантику. Дублирующиеся
	/// имена ключей запрещены — <see cref="ArgumentException"/>. Пустые имена/значения отвергаются.
	/// </summary>
	public InstanceKeys(params ReadOnlySpan<(string Name, string Value)> keys) {
		if (keys.IsEmpty) {
			_entries = EmptyEntries;
			_encodedKey = string.Empty;
			return;
		}
		var builder = ImmutableDictionary.CreateBuilder<string, string>(StringComparer.Ordinal);
		foreach (var (name, value) in keys) {
			ArgumentException.ThrowIfNullOrEmpty(name);
			ArgumentNullException.ThrowIfNull(value);
			if (!builder.TryAdd(name, value)) {
				throw new ArgumentException($"Дублирующийся key-name '{name}'.", nameof(keys));
			}
		}
		_entries = builder.ToImmutable();
		_encodedKey = Encode(_entries);
	}

	private InstanceKeys(ImmutableDictionary<string, string> entries, string encodedKey) {
		_entries = entries;
		_encodedKey = encodedKey;
	}

	/// <summary>
	/// Конструктор для внутреннего использования: переиспользует уже-готовый <see cref="ImmutableDictionary{TKey, TValue}"/>
	/// (например, из <see cref="JobOrchestrator.Internal.InstanceIdentity"/>) и вычисляет ordinal encoded-string.
	/// </summary>
	internal InstanceKeys(ImmutableDictionary<string, string> entries) {
		_entries = entries;
		_encodedKey = entries.Count == 0 ? string.Empty : Encode(entries);
	}

	/// <summary>
	/// Канонический encoded-string (для внутреннего использования: identity, лог, диагностика).
	/// </summary>
	internal string EncodedKey => _encodedKey ?? string.Empty;

	internal ImmutableDictionary<string, string> Entries => _entries ?? EmptyEntries;

	private static string Encode(ImmutableDictionary<string, string> entries) {
		if (entries.Count == 0) return string.Empty;
		var ordered = new KeyValuePair<string, string>[entries.Count];
		int i = 0;
		foreach (var kv in entries) ordered[i++] = kv;
		Array.Sort(ordered, static (a, b) => StringComparer.Ordinal.Compare(a.Key, b.Key));

		var sb = new StringBuilder();
		for (int j = 0; j < ordered.Length; j++) {
			if (j > 0) sb.Append('|');
			AppendEscaped(sb, ordered[j].Key);
			sb.Append('=');
			AppendEscaped(sb, ordered[j].Value);
		}
		return sb.ToString();
	}

	private static void AppendEscaped(StringBuilder sb, string value) {
		foreach (char c in value) {
			if (c is '\\' or '|' or '=') sb.Append('\\');
			sb.Append(c);
		}
	}

	#region IReadOnlyDictionary<string, string>

	/// <inheritdoc/>
	public string this[string key] => Entries[key];

	/// <inheritdoc/>
	public IEnumerable<string> Keys => Entries.Keys;

	/// <inheritdoc/>
	public IEnumerable<string> Values => Entries.Values;

	/// <inheritdoc/>
	public int Count => _entries?.Count ?? 0;

	/// <inheritdoc/>
	public bool ContainsKey(string key) => Entries.ContainsKey(key);

	/// <inheritdoc/>
	public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value) =>
		Entries.TryGetValue(key, out value);

	/// <inheritdoc/>
	public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => Entries.GetEnumerator();

	IEnumerator IEnumerable.GetEnumerator() => Entries.GetEnumerator();

	#endregion

	#region Equality

	/// <inheritdoc/>
	public bool Equals(InstanceKeys other) =>
		string.Equals(_encodedKey ?? string.Empty, other._encodedKey ?? string.Empty, StringComparison.Ordinal);

	/// <inheritdoc/>
	public override bool Equals(object? obj) => obj is InstanceKeys other && Equals(other);

	/// <inheritdoc/>
	public override int GetHashCode() =>
		(_encodedKey ?? string.Empty).GetHashCode(StringComparison.Ordinal);

	public static bool operator ==(InstanceKeys left, InstanceKeys right) => left.Equals(right);
	public static bool operator !=(InstanceKeys left, InstanceKeys right) => !left.Equals(right);

	#endregion

	#region Implicit conversions

	/// <summary>Implicit-конверсия из одиночной пары: <c>stage[("shops", "u1")]</c>.</summary>
	public static implicit operator InstanceKeys((string Name, string Value) tuple) {
		ReadOnlySpan<(string Name, string Value)> span = [tuple];
		return new InstanceKeys(span);
	}

	/// <summary>Implicit-конверсия из span (для variadic-входа): <c>stage[(("k1","v1"), ("k2","v2"))]</c>.</summary>
	public static implicit operator InstanceKeys(ReadOnlySpan<(string Name, string Value)> span) =>
		new InstanceKeys(span);

	#endregion

	/// <inheritdoc/>
	public override string ToString() {
		var enc = _encodedKey ?? string.Empty;
		return enc.Length == 0 ? "[]" : $"[{enc}]";
	}
}
