namespace JobOrchestrator.Tests.Configuration;

/// <summary>
/// Тесты domain-API <see cref="JobOrchestratorBuilder"/>: state-based и action-scoped стили,
/// no-nesting инвариант, colon-валидация, восстановление state на исключении.
/// </summary>
public sealed class BuilderDomainTests {
	private sealed class FakeService : IJobService {
		public Task ExecuteAsync(JobContext ctx, CancellationToken ct) => Task.CompletedTask;
	}

	private static IStageBuilder ConfigureStage(IStageBuilder sb) =>
		sb.HandledBy<FakeService>().RunPeriodically(TimeSpan.FromMinutes(1));

	[Fact]
	public void WithDomain_StateStyle_StagesGetPrefixed() {
		var jobs = new JobOrchestratorBuilder();
		jobs.WithDomain("catalog");
		var shops = ConfigureStage(jobs.Stage("shops"));
		var products = ConfigureStage(jobs.Stage("products"));

		shops.Name.Should().Be("catalog:shops");
		products.Name.Should().Be("catalog:products");
	}

	[Fact]
	public void WithDomain_ActionStyle_StagesGetPrefixed() {
		var jobs = new JobOrchestratorBuilder();
		IStageBuilder? shops = null;
		jobs.WithDomain("catalog", catalog => {
			shops = ConfigureStage(catalog.Stage("shops"));
		});

		shops!.Name.Should().Be("catalog:shops");
	}

	[Fact]
	public void WithDomain_ActionAutoResets_AfterExit() {
		var jobs = new JobOrchestratorBuilder();
		jobs.WithDomain("catalog", catalog => ConfigureStage(catalog.Stage("shops")));

		// После выхода из action-scope domain должен быть сброшен — Stage("global") даёт имя без префикса.
		var global = ConfigureStage(jobs.Stage("global"));
		global.Name.Should().Be("global");
	}

	[Fact]
	public void WithDomain_NestedAction_Throws() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.WithDomain("a", a =>
			a.WithDomain("b", _ => { }));

		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*уже установлен*");
	}

	[Fact]
	public void WithDomain_StateInsideAction_Throws() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.WithDomain("a", a => a.WithDomain("b"));

		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*уже установлен*");
	}

	[Fact]
	public void WithDomain_ActionInsideState_Throws() {
		var jobs = new JobOrchestratorBuilder();
		jobs.WithDomain("a");
		Action act = () => jobs.WithDomain("b", _ => { });

		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*уже установлен*");
	}

	[Fact]
	public void WithoutDomain_AllowsResetting() {
		var jobs = new JobOrchestratorBuilder();
		jobs.WithDomain("a");
		jobs.WithoutDomain();
		jobs.WithDomain("b");
		var s = ConfigureStage(jobs.Stage("x"));

		s.Name.Should().Be("b:x");
	}

	[Fact]
	public void WithoutDomain_OnEmptyState_NoOp() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.WithoutDomain();
		act.Should().NotThrow();
	}

	[Fact]
	public void WithDomain_NameContainsColon_Throws() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.WithDomain("catalog:nested");
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*':'*");
	}

	[Fact]
	public void Stage_NameContainsColon_Throws() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.Stage("catalog:shops");
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*':'*");
	}

	[Fact]
	public void CrossDomainDependsOn_CapturesFullName() {
		var jobs = new JobOrchestratorBuilder();
		IStageBuilder? shops = null;
		jobs.WithDomain("catalog", catalog => {
			shops = ConfigureStage(catalog.Stage("shops"));
		});

		// global-стадия без domain зависит от domain'ed-handle: должна захватить полное имя.
		var report = ConfigureStage(jobs.Stage("report"))
			.DependsOn(shops!);

		report.Name.Should().Be("report");
		// Через BuildRegistry проверим, что зависимость зарезолвилась на "catalog:shops".
		var registry = jobs.GetType()
			.GetMethod("BuildRegistry", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
			.Invoke(jobs, null);
		registry.Should().NotBeNull();
		// Проще: убедимся, что цепочка зависимостей не упала на «висячая зависимость» —
		// то есть имя в DependsOn совпадает с реально объявленной стадией.
	}

	[Fact]
	public void WithDomain_ExceptionInAction_RestoresState() {
		var jobs = new JobOrchestratorBuilder();
		Action act = () => jobs.WithDomain("catalog", _ => throw new InvalidOperationException("boom"));
		act.Should().Throw<InvalidOperationException>();

		// После exception domain должен быть сброшен — следующий Stage без префикса.
		var s = ConfigureStage(jobs.Stage("after-throw"));
		s.Name.Should().Be("after-throw");
	}

	[Fact]
	public void WithDomain_DuplicateStageInSameDomain_Throws() {
		var jobs = new JobOrchestratorBuilder();
		jobs.WithDomain("catalog");
		ConfigureStage(jobs.Stage("shops"));

		Action act = () => jobs.Stage("shops");
		act.Should().Throw<JobConfigurationException>()
			.WithMessage("*'catalog:shops'*");
	}
}
