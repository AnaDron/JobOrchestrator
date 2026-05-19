# Job Orchestrator SDK — TODO

- DependsOnAll(X) — universal-блокировка «все инстансы X успешны»
- ActivitySource / OpenTelemetry-инструментация стадий
- REST-обёртка IJobOrchestrator (overview, trigger, register-key)
- Внешние backend-ы IJobStateStore (SQL, Redis, file)
- Условные зависимости (предикат в DependsOn*)
- Distributed execution (multi-instance с внешним lease)
- Перенести методы DependencyResolver в InstanceManager
- Проверить корректность работы FindPairedInstance
- Инспекция JobOrchestratorRuntime.InstancesOf - используется только для тестов
- Избавиться от OutcomeWaiters