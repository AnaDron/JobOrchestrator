namespace JobOrchestrator.Internal;

/// <summary>
/// Authorization token + data source для <see cref="StageDescriptor.Initialize"/>. Production —
/// <see cref="StageInitializer"/> (создаётся внутри <c>JobOrchestratorBuilder.BuildRegistry</c>);
/// tests — кастомные реализации через <see cref="TestStages"/>-helper.
/// <para>
/// Type-level барьер: для вызова <see cref="StageDescriptor.Initialize"/> нужен экземпляр
/// <see cref="IStageInitializer"/>. Случайно его не получишь — нужно либо пройти весь build-pipeline,
/// либо явно сконструировать fake-реализацию. Это намеренный архитектурный сигнал «эта операция —
/// часть сборки графа, не случайная мутация».
/// </para>
/// </summary>
internal interface IStageInitializer {
	IReadOnlyList<StageDependency> DependenciesOf(StageDescriptor stage);
	IReadOnlyList<string> ExpectedKeyNamesOf(StageDescriptor stage);
	IReadOnlyList<StageDescriptor> DependentsWholeOf(StageDescriptor stage);
	IReadOnlyList<StageDescriptor> DependentsInstanceOf(StageDescriptor stage);
	IReadOnlyList<StageDescriptor> AffectedByKeyRemovalOf(StageDescriptor stage);
	int CancellationRankOf(StageDescriptor stage);
}
