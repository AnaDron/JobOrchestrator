using JobOrchestrator.Configuration.Internal;

namespace JobOrchestrator.Tests.Configuration;

/// <summary>
/// Тесты Configuration-слоя после Phase B/C: проверки <see cref="ConfigurationValidator"/>
/// (structural + per-stage) и computed-полей через <see cref="StageInitializer"/>. Раньше эта
/// логика жила в <c>StageDescriptorGraph</c> и <c>StageRegistry</c>; теперь распределена:
/// валидация — в Configuration, computed-fields — в Internal.
/// </summary>
public sealed class StageRegistryValidationTests {
	private static IReadOnlyList<(string TargetName, DependencyMode Mode)> Deps(params (string, DependencyMode)[] items) => items;

	private static IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> Graph(
		params (string Name, IReadOnlyList<(string TargetName, DependencyMode Mode)> Deps)[] entries
	) {
		var dict = new Dictionary<string, IReadOnlyList<(string, DependencyMode)>>(StringComparer.Ordinal);
		foreach (var e in entries) dict[e.Name] = e.Deps;
		return dict;
	}

	private static StageRegistry RegistryFromGraph(
		IReadOnlyDictionary<string, IReadOnlyList<(string TargetName, DependencyMode Mode)>> rawDeps
	) {
		// Имитация production-pipeline БЕЗ JobOrchestratorBuilder: используется в тестах, которым
		// нужно собрать registry с computed-полями (минуя Fluent API + DI).
		ConfigurationValidator.ValidateGraphStructure(rawDeps);
		var descriptors = new List<StageDescriptor>(rawDeps.Count);
		foreach (var name in rawDeps.Keys) {
			descriptors.Add(new StageDescriptor {
				Name = name,
				ServiceType = typeof(object),
				Interval = TimeSpan.FromMinutes(1),
				RetryPolicy = RetryPolicy.NoRetry,
				Debounce = TimeSpan.Zero,
			});
		}
		var registry = new StageRegistry(descriptors);
		var initializer = new StageInitializer(registry, rawDeps);
		foreach (var d in descriptors) d.Initialize(initializer);
		return registry;
	}

	[Fact]
	public void EmptyGraph_BuildsSuccessfully() {
		var reg = RegistryFromGraph(Graph());
		reg.AllStages.Should().BeEmpty();
	}

	[Fact]
	public void SingleStage_BuildsSuccessfully() {
		var reg = RegistryFromGraph(Graph(("a", [])));
		reg.AllStages.Should().HaveCount(1);
		reg.Get("a").Name.Should().Be("a");
	}

	[Fact]
	public void DuplicateStageNames_Throws_AtRegistryCtor() {
		// Дубль в Configuration невозможен (JobOrchestratorBuilder.Stage блокирует). Defense-in-depth
		// на уровне StageRegistry-ctor — проверяем напрямую.
		var stage = new StageDescriptor {
			Name = "a",
			ServiceType = typeof(object),
			Interval = TimeSpan.FromMinutes(1),
			RetryPolicy = RetryPolicy.NoRetry,
			Debounce = TimeSpan.Zero,
		};
		Action act = () => new StageRegistry([stage, stage]);
		act.Should().Throw<JobConfigurationException>().WithMessage("*Дубль*");
	}

	[Fact]
	public void DanglingDependency_Throws() {
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", Deps(("nonexistent", DependencyMode.Whole)))
		));
		act.Should().Throw<JobConfigurationException>().WithMessage("*nonexistent*");
	}

	[Fact]
	public void SelfCycle_Throws() {
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", Deps(("a", DependencyMode.Whole)))
		));
		act.Should().Throw<JobConfigurationException>().WithMessage("*Цикл*");
	}

	[Fact]
	public void TwoStageCycle_Throws() {
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", Deps(("b", DependencyMode.Whole))),
			("b", Deps(("a", DependencyMode.Whole)))
		));
		act.Should().Throw<JobConfigurationException>().WithMessage("*Цикл*");
	}

	[Fact]
	public void ThreeStageCycle_Throws() {
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", Deps(("b", DependencyMode.Whole))),
			("b", Deps(("c", DependencyMode.Whole))),
			("c", Deps(("a", DependencyMode.Whole)))
		));
		act.Should().Throw<JobConfigurationException>();
	}

	[Fact]
	public void DuplicateLiteralDependency_Throws() {
		// B.DependsOn(A).DependsOn(A) — literal duplicate.
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", []),
			("b", Deps(("a", DependencyMode.Whole), ("a", DependencyMode.Whole)))
		));
		act.Should().Throw<JobConfigurationException>().WithMessage("*дублирующаяся*");
	}

	[Fact]
	public void DuplicateCrossModeDependency_Throws() {
		// B.DependsOn(A).DependsOnInstance(A) — конфликт семантики.
		Action act = () => ConfigurationValidator.ValidateGraphStructure(Graph(
			("a", []),
			("b", Deps(("a", DependencyMode.Whole), ("a", DependencyMode.Instance)))
		));
		act.Should().Throw<JobConfigurationException>().WithMessage("*дублирующаяся*");
	}

	[Fact]
	public void LinearChain_BuildsSuccessfully() {
		var reg = RegistryFromGraph(Graph(
			("a", []),
			("b", Deps(("a", DependencyMode.Whole))),
			("c", Deps(("b", DependencyMode.Whole)))
		));
		reg.AllStages.Should().HaveCount(3);
	}

	[Fact]
	public void DependentsInstance_DetectedSeparately() {
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("products", Deps(("shops", DependencyMode.Instance)))
		));
		var shops = reg.Get("shops");
		shops.DependentsInstance.Should().ContainSingle(s => s.Name == "products");
		shops.DependentsWhole.Should().BeEmpty();
	}

	[Fact]
	public void AffectedByKeyRemoval_TransitiveClosure() {
		// shops → productGroups → products → documents (разные modes, транзитивное замыкание).
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("productGroups", Deps(("shops", DependencyMode.Instance))),
			("products", Deps(("productGroups", DependencyMode.Whole))),
			("documents", Deps(("products", DependencyMode.Whole)))
		));
		reg.Get("shops").AffectedByKeyRemoval.Select(s => s.Name)
			.Should().BeEquivalentTo(["productGroups", "products", "documents"]);
	}

	[Fact]
	public void ExpectedKeyNames_TransitivelyInheritsThroughDependsOn() {
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("productGroups", Deps(("shops", DependencyMode.Instance))),
			("products", Deps(("productGroups", DependencyMode.Whole))),
			("documents", Deps(("products", DependencyMode.Whole)))
		));

		reg.Get("shops").ExpectedKeyNames.Should().BeEmpty();
		reg.Get("productGroups").ExpectedKeyNames.Should().BeEquivalentTo(["shops"]);
		reg.Get("products").ExpectedKeyNames.Should().BeEquivalentTo(["shops"], "products унаследовал измерение от productGroups через DependsOn");
		reg.Get("documents").ExpectedKeyNames.Should().BeEquivalentTo(["shops"], "documents унаследовал то же измерение через цепочку DependsOn");
	}

	[Fact]
	public void ExpectedKeyNames_MultipleDependsOnInstance_MergesInDeclarationOrder() {
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("currencies", []),
			("rates", Deps(("shops", DependencyMode.Instance), ("currencies", DependencyMode.Instance)))
		));
		reg.Get("rates").ExpectedKeyNames.Should().Equal("shops", "currencies");
	}

	[Fact]
	public void ExpectedKeyNames_DirectAndInheritedSameDimension_Deduplicated() {
		// prices: (a) DependsOn(productGroups), которая через DependsOnInstance(shops) уже несёт `shops`;
		// (b) сама объявляет DependsOnInstance(shops). Дедупликация через seen-set.
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("productGroups", Deps(("shops", DependencyMode.Instance))),
			("prices", Deps(("productGroups", DependencyMode.Whole), ("shops", DependencyMode.Instance)))
		));

		reg.Get("prices").ExpectedKeyNames.Should().Equal("shops");
	}

	[Fact]
	public void CancellationRank_LeavesAreLowest() {
		var reg = RegistryFromGraph(Graph(
			("shops", []),
			("productGroups", Deps(("shops", DependencyMode.Instance))),
			("products", Deps(("productGroups", DependencyMode.Whole))),
			("documents", Deps(("products", DependencyMode.Whole)))
		));
		var rankDocs = reg.Get("documents").CancellationRank;
		var rankProducts = reg.Get("products").CancellationRank;
		var rankPg = reg.Get("productGroups").CancellationRank;
		var rankShops = reg.Get("shops").CancellationRank;
		rankDocs.Should().BeLessThan(rankProducts);
		rankProducts.Should().BeLessThan(rankPg);
		rankPg.Should().BeLessThan(rankShops);
	}
}
