using Xunit;

// Все integration-тесты запускают TestHost (DI scope, ThreadPool, Channel). Параллельное выполнение
// делает их timing-зависимыми друг от друга. Последовательное выполнение даёт стабильность за счёт ~5-10s
// общего времени прогона assembly.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
