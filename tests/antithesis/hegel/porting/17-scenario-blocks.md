# 17 — Scenario blocks

Ports `MD/singleton_driver_scenario_blocks` (the loop in `W/internal/block/block.go`) and the
six registered `pkg/scenario` block groups (main-drivers §3.35, HEGEL-INVENTORY "Registered
block checklist" 26/26). Scenario ledgers (`marketplace`, `gaming`, `lending`, `billing`,
`treasury`, `dept-*`, …) are outside the oracle's partition (DESIGN §3); blocks stay unkeyed and
go through the client's `lib` route (production retry policy), as in the old driver.

Code:

- `W/bin/client/ext_block.go`: `POST /block-setup {"opId","group","timeoutMs"}` and
  `POST /block {"opId","name","values":["<u64>",…],"timeoutMs"}`. The groups come straight
  from `pkg/scenario` (`MarketplaceBlocks()` … `MultiLedgerPayrollBlocks()`); `W/internal/block`
  is not imported. The random source is a value list (`valueRandom`): `Uint64` pops, `Intn`
  and `Int63n` apply `scenario.RandFunc`'s one-draw modulo to a popped value, an exhausted list
  returns 0 and sets `exhausted`. Every RPC a block sends goes through a classifying
  `grpc.ClientConnInterface` wrapper (`client: every RPC error the client returns is
  classified`), and every completion is reported over UDP like `/op`. A block panic is answered
  as a non-local `Unknown` error, so the driver reports `block <name> failed`.
- `HG/driver/src/rules/blocks.rs`: setup hook `blocks_setup`, rule `block_run` (weight 1.5),
  and `scenario_blocks!`, which declares the 26 names and expands to one literal
  `check_reachable!` and one literal `check_unreachable!` per block.

## Old assertions

| Old source | Old assertion (kind: literal) | New | Where |
|---|---|---|---|
| W/internal/block/block.go:106 | Always: should be able to setup scenario | verbatim (one evaluation per group; a non-transient failure, or no success within the 120 s hook budget, is `false`) | rules/blocks.rs `blocks_setup` |
| W/internal/block/block.go:135 | Unreachable: scenario block generation failed | dropped: no hegel callback runs in the client any more; the driver draws the values itself, and a block panic is reported as `block <name> failed` | – |
| W/internal/block/block.go:189-193 (`emitBlockSucceeded`, AssertRaw, 26 names) | Reachable: block `<name>` succeeded | verbatim, 26 literal `check_reachable!` calls produced by `scenario_blocks!` | rules/blocks.rs `block_run` |
| W/internal/block/block.go:195-198 (`emitBlockFailed`, AssertRaw, 26 names) | Unreachable: block `<name>` failed | verbatim, 26 literal `check_unreachable!` calls; classification **loosened**: DeadlineExceeded, Canceled and client-local errors are transient (nothing), because the harness imposes a per-call deadline (`timeoutMs` 8000) that the old loop did not (DESIGN Appendix A C11) | rules/blocks.rs `block_run` |
| W/internal/checks.go:66 (via `block.CheckPostCommitVolumes`) | Always: post-commit volume entry has missing required fields | renamed: `a block's post-commit volumes are well formed` (Always), evaluated once per succeeded block, false at the first malformed entry | rules/blocks.rs `block_run` |
| W/internal/checks.go:72 | Always: post-commit volume input is invalid | merged into `a block's post-commit volumes are well formed` | rules/blocks.rs |
| W/internal/checks.go:78 | Always: post-commit volume output is invalid | merged into `a block's post-commit volumes are well formed` | rules/blocks.rs |
| W/internal/checks.go:14 (`CheckVolume` from `CheckPostCommitVolumes`) | Always: reported balance and volumes should be consistent | dropped: on post-commit volumes the balance is computed as `input − output` by the check itself, so the comparison is a tautology there | – |
| W/internal/apply.go:30 (via `ExtractCreatedTransaction`) | Unreachable: CreatedTransaction must carry its transaction payload | dropped here: a created transaction without its payload simply has no post-commit volumes to check. The oracle's commit checks cover the shape on tracked ledgers | – |

The PCV check now covers every created transaction of a block's Apply response, not only the
first log as `ExtractCreatedTransaction` did (a block Apply with two orders, e.g.
`lending/repay`, is fully checked). This is under the new literal, not the old ones.

Not ported: the `SCENARIO` environment filter of the old driver (empty / one / comma-separated
scenarios). Every group is always set up and every block is eligible; hegel's equal-weight
choice among the 26 replaces it.

## New properties

| Literal | Kind | Where |
|---|---|---|
| `a block runs within its pre-drawn values` | Sometimes (`exhausted == false`), every answered `/block` | rules/blocks.rs `block_run` |
| `a block's post-commit volumes are well formed` | Always | rules/blocks.rs `block_run` |
| `the client program accepts every block request` | Unreachable (the client answered `/block` with a non-200) | rules/blocks.rs `post_block` |

## Reachability

| Must-hit | Reached by | Needs |
|---|---|---|
| `should be able to setup scenario` | `blocks_setup` (setup hook, 6 evaluations) | no faults |
| `block <name> succeeded` × 26 | `block_run`: every block is drawn with equal weight; the 16 values make a success likely once the group's ledger has funds. Blocks that read balances (`purchase`, `payout`, `buy_item`, `p2p_trade`, `clawback`, `disburse`, `repay`, `provision`, `write_off`, `charge`, `recognize`, `fx`, `vendor_pay`, `distribute`, `pay_salary`, `cost_alloc`, `revert`) skip until a funding block of the same group succeeded; in a long run every funding block is drawn many times | no faults (long enough run) |
| `a block's post-commit volumes are well formed` | any succeeded block | no faults |
| `a block runs within its pre-drawn values` | any answered `/block` (the largest block uses 5 of the 16 values) | no faults |

## Requests to 99

- None for code. A short native run may not hit all 26 `block … succeeded` Reachables (some
  depend on a prior funding block of the same group); task 99 should judge them on the long
  local run, not delete them.
