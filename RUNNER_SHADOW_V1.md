# Runner Score V1 / Non-Runner Veto V1 — SHADOW

Issue: #14

## Purpose

This layer measures whether Candidate/EARLY events can identify strong continuation runners earlier than the classic Premium path, while rejecting local-top/non-runner entries. It is research-only and does not change production Premium thresholds, order execution, sizing, stop/TP logic, startup preflight, or LIVE safety.

The core distinction is deliberate:

1. **Runner selection:** is this coin likely to continue strongly?
2. **Entry confirmation:** is the next 15–30 seconds executable, or is the move rejecting?

A strong coin is not automatically a good immediate entry.

## Runner Score V1

Candidate, public EARLY and classic Premium stages feed a deterministic 0–100 score. The score is explainable; raw features, per-feature contributions and reason codes are stored.

Positive priors include price acceleration (`chg30/chg60`), BTC-relative strength, strong Gainers rank, efficient/moderate flow and balanced aggressive buy participation. Risk penalties include extreme/inefficient flow, >80% buy concentration, one-sided book imbalance, large already-run distance, higher phase risk and materially negative OI.

Existing Momentum / Entry / Rise values are context features only in this model; they are not replacement production gates.

## FAST RUNNER watch

A public EARLY event with Runner Score >= 70 opens a persistent FAST watch. The classic candidate episode may reset without deleting this wave memory. The watch remains eligible for reacquire for up to 30 minutes.

The first 15 seconds are evaluated forward-only. Strong score plus real progress can produce `ALLOW`; rejection can move the watch into reacquire state; inconclusive behavior waits for 30 seconds.

The 30-second check can rescue a setup that was inconclusive at 15 seconds when MFE/progress builds with controlled drawdown. Otherwise the system keeps wave memory for a possible reacquire instead of pretending the coin ceased to exist.

## REACQUIRE watch

A later Candidate can open a `REACQUIRE` child watch when it occurs after the initial 30-second window but inside the persistent watch horizon and contemporaneous strength remains acceptable. The child keeps an explicit parent-watch link.

This is designed to measure CAP-style behavior: strong EARLY -> shakeout/reset -> renewed relative strength -> continuation.

## Non-Runner Veto V1

The 15/30-second decisions are stored in `non_runner_veto_v1_shadow`. They use only prices observed after the shadow stage became visible. Restart gaps cannot be retroactively treated as clean confirmations; incomplete paths are marked gapped and revert to reacquire-only observation.

Research labels at 60 minutes:

- `RUNNER`: MFE >= +6%
- `NON_RUNNER`: MFE < +2%
- `GRAY`: +2% <= MFE < +6%
- `GAPPED`: observation continuity was broken, so the clean label is withheld

## Telegram

When an entry-quality shadow watch reaches `ALLOW`, the private admin chat can receive one of:

- `FAST RUNNER PREMIUM — SHADOW`
- `REACQUIRE PREMIUM — SHADOW`

These messages are intentionally not routed through the public broadcast wrapper and do not call AutoTrade. They are labelled TEST/SHADOW and never place an order. `RUNNER_SCORE_V1_NOTIFY` defaults to `0` (OFF), including at startup, without disabling telemetry. Only an explicit `1`, `true`, `yes`, or `on` (case-insensitive, whitespace trimmed) enables these private research messages; empty or unrecognized values stay OFF. The verified startup log records the effective `runner_score_v1_notify` value.

## Additive schema

- `runner_score_v1_shadow`
- `runner_watch_v1_shadow`
- `non_runner_veto_v1_shadow`
- `runner_watch_outcome_v1_shadow`

Migrations are `CREATE TABLE/INDEX IF NOT EXISTS`; no legacy table is reset, dropped or replaced.

## Named regression intent

- **CAPUSDT:** high-priority runner candidate; 7.2x flow must not be blindly penalized when price acceleration, BTC-relative strength, Gainers rank and buy balance agree.
- **CYS-style:** 15-second inconclusive behavior may become `ALLOW` on genuine 30-second rescue/progress.
- **MTL-style:** exhaustion/one-sided/weak-progress context must not become a FAST runner merely because the legacy headline score is high.

## Promotion policy

This is not a production strategy. Compare it with classic Premium for multiple complete days. Key metrics are top-decile runner precision/recall, missed-runner recovery, BLOCK precision, false-block runner rate, shadow-vs-classic Premium lead time and day-by-day stability. Production changes require a separate evidence-backed decision.
