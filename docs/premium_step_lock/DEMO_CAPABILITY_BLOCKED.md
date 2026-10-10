# Demo capability candidate — stopped before exchange writes

Base: `9d115152408bf971e587aab2a04d10545f748d00`.
Work: `2026-10-10_Premium_Step_Lock_V1_Safe_Stop_Replacement_Capability_v1`.

Dedicated Demo credentials authenticated successfully using read-only requests.
The initial account had zero positions, normal orders and algo orders, one-way mode.
No credential values are stored in this checkout.

The first offline probe prerequisite suite ran 42 tests: 41 passed, one error:
`test_demo_capability_guards.Flow.test_standard_ordering_and_cleanup`.
The test expected successful final cleanup but the returned cleanup record was null.
Static control-flow review identifies the cause: standard replacement cancels the old
stop and records its ID in `cancelled`; final cleanup calls `cancel` for that same
already-terminal old stop. `cancel` checks the duplicate-attempt set before querying
the terminal status, so it raises `DUPLICATE_CANCEL_DENIED`. This is a local harness
defect, not evidence that Binance supports or rejects closePosition overlap.

The candidate and failing test are retained unchanged. No patch-and-retest, alternate
stop semantics, Demo order cycle, production adapter, certificate or deployment is
authorized by this failed prerequisite. No second candidate is started in this Work.
The next separate action is `REVISE_OR_REJECT_PREMIUM_STEP_LOCK_V1_CAPABILITY_IMPLEMENTATION`.

Main result:
`PREMIUM_STEP_LOCK_V1_CAPABILITY_BLOCKED_REGRESSION_OR_STATE_FAILURE`.

`research/premium_step_lock_demo_capability.py` is an unbound, failed first-cycle
prototype, not an operational entry point. It imports no bot/config/database and
has no main-loop launcher. It was never dispatched to the credential holder.
The source documents a bounded technical witness (existing fixture plan 100/98.7
mapped to actual fill, then a higher below-market test stop); it does not implement
or optimize the frozen economic step profile.

Existing Premium state machine, profile, bot runtime, CURRENT_TP2 default and hard
LIVE block are unchanged. The 42 offline guards are not a substitute for the Work's
adapter/certificate/restart/response-loss acceptance matrix. Those conditional
stages remain NOT_EXECUTED after this failure. Actual stop trigger: NOT_OBSERVED.

Existing focused baseline: 144 passed with zero external network calls. An earlier
dependency-read failure under the Windows sandbox is separately retained; changing
only process access resolved it without modifying source, fixtures or criteria.
Full-suite baseline and historical failures are preserved in the research archive;
no full-suite PASS or new clean capability commit comparison is claimed.
