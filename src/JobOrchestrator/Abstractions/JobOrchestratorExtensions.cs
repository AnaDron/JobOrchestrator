namespace JobOrchestrator.Abstractions;

/// <summary>Удобные расширения над <see cref="IJobOrchestrator"/>/<see cref="IInstanceHandle"/>.</summary>
public static class JobOrchestratorExtensions {
	/// <summary>
	/// Все зарегистрированные стадии оркестратора без разбора домена. SelectMany по доменам —
	/// порядок: сначала стадии root-домена, затем по доменам в порядке регистрации.
	/// </summary>
	public static IReadOnlyCollection<IStageHandle> AllStages(this IJobOrchestrator self) {
		ArgumentNullException.ThrowIfNull(self);
		var result = new List<IStageHandle>();
		foreach (var domain in self) {
			foreach (var stage in domain) result.Add(stage);
		}
		return result;
	}

	/// <summary>
	/// Lookup стадии по полному имени (<c>"domain:local"</c> или просто <c>"local"</c> для root-домена).
	/// Удобство для full-name из конфигурации/логов — без ручного парсинга domain-separator'а.
	/// </summary>
	/// <exception cref="ArgumentException">Если стадии с таким полным именем нет.</exception>
	public static IStageHandle GetStage(this IJobOrchestrator self, string fullName) {
		ArgumentNullException.ThrowIfNull(self);
		ArgumentException.ThrowIfNullOrEmpty(fullName);
		foreach (var domain in self) {
			foreach (var stage in domain) {
				if (string.Equals(stage.Name, fullName, StringComparison.Ordinal)) return stage;
			}
		}
		throw new ArgumentException($"Стадия с полным именем '{fullName}' не зарегистрирована.", nameof(fullName));
	}

	/// <summary>
	/// Ждать первого успешного завершения итерации инстанса. Memoized: если на момент вызова
	/// <see cref="InstanceInfo.LastSuccess"/> уже не <c>null</c>, Task завершён сразу. Поддерживает
	/// late-register: handle может быть для ещё не материализованного инстанса — ожидание подхватит
	/// его появление через <see cref="IStageHandle.Changes"/> и дождётся первой success-итерации.
	/// <para>
	/// Если инстанс удалён каскадом до первого успеха либо оркестратор остановлен — бросает
	/// <see cref="InvalidOperationException"/>.
	/// </para>
	/// <para>
	/// <b>Реализация через broadcaster'ы (без отдельного TCS-реестра):</b>
	/// </para>
	/// <list type="number">
	/// <item><b>Fast-path</b> — проверка <c>Snapshot.LastSuccess</c>; если уже success — return.</item>
	/// <item><b>Late-materialization</b> — если <c>Snapshot == null</c>, подписаться на
	/// <see cref="IStageHandle.Changes"/>, дождаться <c>Added</c> для нашей identity (replay-семантика
	/// поймает уже-материализованный случай).</item>
	/// <item><b>Capture running iteration</b> — после subscribe на iteration stream, прочесть
	/// <see cref="IInstanceHandle.RunningIteration"/> (ловит итерацию, начавшуюся до subscribe и
	/// потому отсутствующую в нашем broadcaster-канале) и дождаться её Completion.</item>
	/// <item><b>Stream loop</b> — ждать future итераций через <c>await foreach</c>; первая, чей
	/// <see cref="IIterationHandle.Completion"/> завершён без exception, — success.</item>
	/// </list>
	/// <para>
	/// Re-check <c>Snapshot.LastSuccess</c> между фазами закрывает microscopic race-windows.
	/// </para>
	/// </summary>
	public static async Task WaitForSuccessAsync(this IInstanceHandle handle, CancellationToken ct = default) {
		ArgumentNullException.ThrowIfNull(handle);

		// 1. Fast-path: уже был success.
		if (handle.Snapshot?.LastSuccess is not null) return;

		// 2. Late-materialization: подписаться на stage.Changes ДО проверки Snapshot.
		//    Replay даст Added для уже-материализованного handle; live-loop поймает late-materialization.
		if (handle.Snapshot is null) {
			await foreach (var change in handle.Stage.Changes.WithCancellation(ct).ConfigureAwait(false)) {
				if (change.Kind == StageChangeKind.Added && change.Instance.Equals(handle)) break;
			}
			// Re-check после materialization — success мог произойти в окне subscribe→Added.
			if (handle.Snapshot?.LastSuccess is not null) return;
		}

		// 3. Capture running iteration ДО ухода в stream loop.
		//    RunningIteration ловит итерацию, начавшуюся ДО subscribe (handle отсутствует в нашем канале);
		//    stream loop ловит итерации, начавшиеся ПОСЛЕ subscribe.
		await using var iterEnum = handle.GetAsyncEnumerator(ct);
		var current = handle.RunningIteration;
		if (current is not null) {
			// WaitAsync(ct) — без неё cancel-токен caller'а не доходит до running-итерации, и waiter
			// висит до её естественного завершения. Остальные phase'ы ct уважают (WithCancellation /
			// MoveNextAsync(ct)); Phase 3 без WaitAsync был единственным «глухим» местом.
			try { await current.Completion.WaitAsync(ct).ConfigureAwait(false); return; }
			catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
			catch (IterationFailedException) { /* fall through to stream */ }
		}
		// Re-check после RunningIteration — success мог завершиться в окне между фазами.
		if (handle.Snapshot?.LastSuccess is not null) return;

		// 4. Stream loop — ждём future итераций.
		while (await iterEnum.MoveNextAsync().ConfigureAwait(false)) {
			try { await iterEnum.Current.Completion.ConfigureAwait(false); return; }
			catch (IterationFailedException) { /* continue */ }
		}

		throw new InvalidOperationException(
			$"Инстанс {handle.FullyQualifiedName} не достиг успеха: iteration stream завершён (cascade-removal или shutdown оркестратора).");
	}
}
