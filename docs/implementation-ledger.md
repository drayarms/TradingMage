# Implementation ledger
Plan: docs/superpowers/plans/2026-10-09-golden-cross-dip-recovery-plan.md
Preflight: engine settings and history snapshots feed both adapters; charts consume serializable result rows.
Ruling: use an isolated scratch repository from the supplied files; no external Git repository was provided.
Task 1: complete — engine tests watched fail on missing module, then pass.
Task 2: complete — history adapter tests watched fail on missing module, then pass.
Task 3: complete — event adapter tests watched fail on missing module; full-run accounting verified with synthetic provider.
Task 4: complete — fake-broker tests watched fail on missing module; disabled flag, pending exposure, dedupe and full exits verified.
Task 5: complete — indicator toggle tests watched fail on missing keyword, then pass; endpoint forwarding, compileall and bash syntax verified.
Ruling: use a dedicated simulation adapter before legacy numeric-exit validation rather than assign an arbitrary exit strategy number — preserves legacy accounting and avoids implying a numeric strategy exit — risk if wrong: separate adapter may miss a legacy reporting field; end-to-end JSON/accounting tests cover its output.
Ruling: offline import shims and AST endpoint-body tests replace unavailable FastAPI/Redis/Alpaca runtime integrations in this environment — no credentials or runtime dependencies were provided — risk if wrong: real SDK/HTTP compatibility remains unverified.
Ruling: live orders use day limits and persistent exit reconciliation, while simulations retain existing immediate bid/ask fills — caps require bounded purchase prices and long-term exits need retries after expired orders — risk if wrong: simulations may overstate live fill rate, explicitly documented.
Verification: python3 -m unittest discover -s tests -v passed 27 tests; compileall app and bash -n run_local_endpoint.sh passed.
Final review: independent reviewer identified Critical exit-fill retry race and Important crossover-prehistory bypass, per-symbol exit failure isolation and shared simulation flags.
Final: fixed exit-fill retry race — test_exit_fill_during_reconciliation_never_sells_twice RED→GREEN.
Final: fixed insufficient startup history — test_startup_cross_is_not_tradeable RED→GREEN; suppress unqualified cross in the shared causal engine so reporting-period crosses are protected too.
Final: fixed exit reconciliation isolation — test_one_exit_failure_does_not_block_other_symbols RED→GREEN.
Final: fixed concurrent recording flags — test_run_does_not_change_shared_backtester_flags RED→GREEN.
Final: regraded delayed exit timestamps as Important because entry/exit charts otherwise misstate execution chronology; fixed — test_delayed_exit_uses_fill_timestamp RED→GREEN.
Final verification: full suite 32/32 passed. No deferred minor findings.
