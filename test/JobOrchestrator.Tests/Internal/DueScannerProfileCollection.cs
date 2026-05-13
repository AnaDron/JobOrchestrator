using System.Diagnostics.CodeAnalysis;

namespace JobOrchestrator.Tests.Internal;

/// <summary>
/// Изолированная коллекция для timing-чувствительных профайл-тестов: запускается СЕРИАЛЬНО относительно
/// остальных коллекций тестового assembly. Без этого <see cref="DueScannerProfileTests"/> деградирует
/// по wall-clock-time при параллельной нагрузке на CPU от других тестов и даёт false-positive regression alerts.
/// </summary>
[CollectionDefinition("DueScannerProfile", DisableParallelization = true)]
[SuppressMessage("Naming", "CA1711", Justification = "xUnit convention: collection-definition class сохраняет суффикс \"Collection\" для отражения семантики Collection-аттрибута.")]
public sealed class DueScannerProfileCollection { }
