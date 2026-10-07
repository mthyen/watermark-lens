# Future work

Ideas deliberately deferred during initial development, not forgotten.

## Checkpoint-safe tap operator state

`WatermarkTapOperator`'s state (`currentWatermark`, the per-bucket accumulators) is currently
plain instance fields, not checkpointed. On restart, this resets: `currentWatermark` goes back to
uninitialized until the next real watermark arrives, and whatever partial bucket was
accumulating at the moment of the restart is lost rather than emitted. Acceptable for now since
this is diagnostic data, not the wrapped job's actual output. If it ever needs to survive
restarts, this means adopting `CheckpointedFunction` and registering the state via
`OperatorStateStore`/`ListStateDescriptor`.

## Per-event-type correlation for lateness

Right now nothing in the observation model can answer "are events of a particular domain type
(e.g. `OrderCancelled` vs `OrderPlaced`) disproportionately late?" The tap operator is generic
over `T` and has no way to classify what kind of business event an element represents.

The `late_drop` event's existing `key` field already gives partial value here for free (assuming
the wrapped stream is keyed in a way that correlates with what the user cares about), but
`event_bucket` deliberately does not break down by type - that's aggregate, high-volume,
per-subtask-per-second data, and breaking it down by an unbounded, user-defined type dimension
would fight the "sample/aggregate at high volume" goal directly.

If this becomes worth building: accept an optional classifier function (e.g.
`Function<T, String>`) in `WatermarkLensConfig`, and thread its output into `late_drop` records
(and cautiously, if cardinality allows, `event_bucket`).
