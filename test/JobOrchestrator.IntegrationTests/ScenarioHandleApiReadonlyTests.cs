using JobOrchestrator.IntegrationTests.Support;

namespace JobOrchestrator.IntegrationTests;

/// <summary>
/// Чтение-only сценарии handle-API: топология стадий и identity-handle'ы. Не запускают
/// итерации, не эмитят ключи, не вызывают RunAsync/RegisterKey — поэтому могут разделять
/// один <see cref="IHost"/> на класс через class-fixture'ы (по одной на используемый граф).
/// <para>
/// Сокращение Host.Build/Start/Stop циклов снижает GC/ThreadPool-нагрузку test-runner'а;
/// см. <see cref="KeylessAHostFixture"/> для обоснования read-only-shared-host подхода.
/// </para>
/// </summary>
public sealed class ScenarioHandleApiReadonlyTests(KeylessAHostFixture keyless, ShopsPgHostFixture shopsPg)
	: IClassFixture<KeylessAHostFixture>, IClassFixture<ShopsPgHostFixture> {

	#region keyless graph (Stage "a")

	[Fact]
	public void Indexer_KnownStage_ReturnsHandle() {
		var handle = keyless.Orchestrator.Root["a"];
		handle.Should().NotBeNull();
		handle.Name.Should().Be("a");
	}

	[Fact]
	public void Indexer_UnknownStage_Throws() {
		Action act = () => _ = keyless.Orchestrator.Root["nonexistent"];
		act.Should().Throw<ArgumentException>().WithMessage("*nonexistent*");
	}

	[Fact]
	public void Indexer_SameStageHandle_Cached() {
		var h1 = keyless.Orchestrator.Root["a"];
		var h2 = keyless.Orchestrator.Root["a"];
		ReferenceEquals(h1, h2).Should().BeTrue("StageHandle cached в Runtime");
	}

	#endregion

	#region shops + pg graph

	[Fact]
	public void State_NonexistentKeyedInstance_ReturnsNull() {
		// Для keyed-стадии без emitted-ключей инстансов нет → State == null.
		// shops запускается, но keys не эмитит — pg-инстанса с ("shops","u1") нет.
		var state = shopsPg.Orchestrator.Root["pg"][("shops", "u1")].State;
		state.Should().BeNull("инстанс не материализован");
	}

	[Fact]
	public void Indexer_InvalidKeyName_ThrowsArgumentException() {
		// orchestrator.Root["pg"][("wrong-key", "v")] → fail-fast в момент handle-construction,
		// а не silent-NotFound в RunAsync.
		Action act = () => _ = shopsPg.Orchestrator.Root["pg"][("nonexistent-key", "v")];
		act.Should().Throw<ArgumentException>().WithMessage("*nonexistent-key*");
	}

	[Fact]
	public void Indexer_WrongKeyCount_ThrowsArgumentException() {
		// pg ожидает 1 key (shops); передаём 2 → fail-fast.
		Action act = () => _ = shopsPg.Orchestrator.Root["pg"][new InstanceKeys(("shops", "u1"), ("extra", "v"))];
		act.Should().Throw<ArgumentException>().WithMessage("*ожидает 1*передано 2*");
	}

	[Fact]
	public void Indexer_KeylessOnKeyedStage_ThrowsArgumentException() {
		// orchestrator.Root["pg"][InstanceKeys.Empty] на стадии с зависимостями → fail-fast.
		Action act = () => _ = shopsPg.Orchestrator.Root["pg"][InstanceKeys.Empty];
		act.Should().Throw<ArgumentException>().WithMessage("*ожидает 1*передано 0*");
	}

	[Fact]
	public void InstanceHandle_Equality_BasedOnIdentity() {
		// Два handle на один и тот же логический инстанс → value-equality.
		// Инстансы не материализованы — handle'ы on-demand, но Equality по Identity всё равно работает.
		var h1 = shopsPg.Orchestrator.Root["pg"][("shops", "u1")];
		var h2 = shopsPg.Orchestrator.Root["pg"][("shops", "u1")];
		var h3 = shopsPg.Orchestrator.Root["pg"][("shops", "u2")];

		h1.Equals(h2).Should().BeTrue("одинаковая Identity → value-equality");
		h1.GetHashCode().Should().Be(h2.GetHashCode());
		h1.Equals(h3).Should().BeFalse("разные keys");

		// Также проверим использование в HashSet — типовой scenario.
		var set = new HashSet<IInstanceHandle>([h1, h2, h3]);
		set.Should().HaveCount(2, "h1 ≡ h2, h3 — отдельный");
	}

	#endregion
}
