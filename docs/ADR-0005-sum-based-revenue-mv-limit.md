# ADR-0005: Accept the sum-based revenue MV limit and read exact revenue from the raw table

Status: Accepted (known limit, no fix in scope)

## Context

`orders_per_minute` is an AggregatingMergeTree fed by `orders_per_minute_mv`. It holds two measures per minute, shop and event type:

- `orders`, stored as `uniqExactState(event_id)`. Merging these states is exact, so replays do not change the count.
- `revenue_cents`, stored as `sumIf(total_cents, event_type = 'ORDER_PAID')`. The MV sums every row it receives and has no `event_id` awareness.

A full replay can insert a block that the raw table still accepts (after the dedupe window has passed, or with different batch boundaries). The raw table collapses those rows on merge, but the MV has already added the revenue.

## Decision

- Keep the sum-based MV for the live revenue panel, and label it as a materialized view.
- Make the dashboard's "Exact revenue" panel read `orders_raw FINAL`, which is correct after a merge.
- `scripts/replay-demo.sh` prints a reconciliation query (MV total minus raw FINAL total). The difference is reported and does not fail the run.

## Consequences

- After a replay, the MV revenue can be higher than the true value. This is documented, not hidden, and the reconciliation query shows it.
- Fixing it would mean keying revenue by `event_id` or rebuilding the MV. Neither is in scope for this project.
- No revenue figures are published in the docs. A real difference can only be measured on a live stack.
