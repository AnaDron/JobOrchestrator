# Job Orchestrator SDK — TODO

- IDomainHandle.Changes — domain-level life-cycle observer
- TryGet методы на IJobOrchestrator/IDomainHandle/IStageHandle для probe-сценариев без exception
- Абстрагировать pattern cache+gen-tagged replay в GenerationalCache<TKey, TValue> при появлении третьего broadcaster'а
- DependsOnAll(X) — universal-блокировка «все инстансы X успешны»
- ActivitySource / OpenTelemetry-инструментация стадий
- REST-обёртка IJobOrchestrator (overview, trigger, register-key)
- Внешние backend-ы IJobStateStore (SQL, Redis, file)
- Условные зависимости (предикат в DependsOn*)
- Distributed execution (multi-instance с внешним lease)
- Инспекция JobOrchestratorRuntime.InstancesOf - используется только для тестов