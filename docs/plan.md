# Cross entry revamp Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans. Steps below track inline implementation and review.

**Goal:** Implement the approved cross-only entry revamp and preserve range/exits.
**Architecture:** Shared pure cross evaluator plus completed-bar gradient annotation, portfolio fill spacing, continuous candidate scheduling, and cross-only delayed-fill revalidation.
**Tech Stack:** Existing Python, pandas, unittest, FastAPI and shell runner.
**Spec:** docs/design.md

## Global Constraints
- Simulation focus; no live orders or external historical backtest.
- Tabs; no added production dependencies; range/exits and current execution phase order preserved.
- Default entry_trigger=none; enforce_atr_invalidation=false; completed native4h index spacing of two.

## Review Focus
- Gradients cannot expose unconfirmed points; missing points permit aligned branch only.
- Inverse sizing uses existing directional references, not a new opposite reference.
- Continuous candidate direction must not consume the same-timestamp signal-exit dedupe key.
- Delayed cross fills cannot bypass current eligibility/spacing or increase submitted quantity.
- Legacy tests for intentionally removed cross rules need updated expectations, not altered range behavior.

## Task 1: pure eligibility and settings
Files: app/golden_cross_dip_recovery.py, new app/cross_entry_policy.py, tests/test_cross_entry_revamp.py.
Interfaces: evaluate_cross_entry(side,price,base,snapshot,exposure,pending,settings)->decision; attach_cross_turning_context(frame,points)->frame.
- [x] Write failing tests for regime truth table, mandatory market exclusions, price-only confirmation, ATRtoggle, inverse sizing and valid directional references.
- [x] Run those tests RED, implement policy/settings/wrappers, rerun GREEN.

## Task 2: completed context and fill spacing
Files: app/golden_cross_history.py (baseline dependency), app/golden_cross_simulation.py, app/strategy_portfolio.py, tests/test_cross_entry_execution.py.
Interfaces: portfolio.cross_entry_decision(ticker,side,when,price)->decision; cross_entry_spacing_reason(ticker,side,when)->str|None; successful claim_entry records crossfill index only.
- [x] Write failing context/spacing tests; verify RED.
- [x] Annotate gradient by confirmed4h points, change historical readiness to side-specific references; add spacing and continuous candidate path; verify GREEN.

## Task 3: pending fills
Files: app/backtest_limit_orders.py; tests/test_cross_entry_execution.py.
- [x] Write failing delayedfill cancellation/quantity/spacing and unchanged range tests; verify RED.
- [x] Cross-only revalidate before fill using current row/limit, cancel invalid cross orders and reduce quantities as needed; verify GREEN.

## Task 4: endpoints, compatibility, command and regression
Files: app/app.py, app/backtester.py, run_range_cross_test.sh, affected existing tests.
- [x] Failing endpoint tests for none default and ATRtoggle forwarding; verify RED.
- [x] Wire options, deprecated filter compatibility, updated command from approved design.
- [x] Update tests whose prior cross-entry requirements were explicitly replaced. Run fullsuite, compile, shellsyntax.

## Task 5: review and delivery
- [x] Fresh reviewer against design, changes and tests; address important issues with regressions.
- [x] Package complete changed files, tests, command, design, plan and concise README. Save deliverable and report checks/limitations.
