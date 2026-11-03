# Stretch and exit filters Implementation Plan

> Execute natively with superpowers:executing-plans; implementation explicitly authorized.

**Goal:** Add approved cross entry filters and universal remembered exit veto.
**Architecture:** causal shared annotations, pure entry checks and portfolio close guard; reuse all execution/accounting.
**Tech Stack:** Python/pandas/unittest, existing endpoint and shell runner.
**Spec:** docs/stretch_and_exit_design.md

## Global Constraints
- Cross entries only; universal veto for cross/range. Completed 4h context; no future data.
- Strict inequalities; finite positive endpoint divisor defaults 2. Tabs and existing dependencies.
- Pending exit belongs to exact position; first permitted execution under existing session/limit rules.

## Review Focus
- Prefix causality and indicator-specific turning references.
- Range/opposite stretch resets, strict wick and body boundaries.
- Veto zero/missing gradients, both signs, both owners, replacement exits.
- Veto reappearing before a limit fill, order expiration, stale intents after close/reopen.
- Native range crosses independent of cross exit method and no retroactive exits.

## Tasks
- [x] Write failing tests for stretch memory, body comparison, ATR boundary/divisor and causal gradients. Implement annotations/policy, verify.
- [x] Write failing tests for universal veto/remembered release, position lifetime and delayed fill reblock. Implement central guard, retry and order binding, verify.
- [x] Add range-cross completion tests and scheduling; verify no historical crossing used against new positions.
- [x] Wire endpoint/default/validation with failing tests; update runner, snapshot fields and fixtures for intentional new rules.
- [x] Run full suite, compilation and shell checks; review diff, package and replace existing deliverable.
