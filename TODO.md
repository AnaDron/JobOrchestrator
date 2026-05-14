# Job Orchestrator SDK — TODO

- DependsOnAll(X) — universal-блокировка «все инстансы X успешны»
- ActivitySource / OpenTelemetry-инструментация стадий
- REST-обёртка IJobOrchestrator (overview, trigger, register-key)
- Внешние backend-ы IJobStateStore (SQL, Redis, file)
- Условные зависимости (предикат в DependsOn*)
- Distributed execution (multi-instance с внешним lease)
- GlobalConcurrency limit (поверх per-stage WithConcurrencyLimit)
- StageDescriptor вместо string как ключ в internal-словарях (ConcurrencyLimits, KeyspaceRegistry, InstanceManager)
- SnapshotByStage → IReadOnlyList<EmitterBucket> вместо yield-IEnumerable
