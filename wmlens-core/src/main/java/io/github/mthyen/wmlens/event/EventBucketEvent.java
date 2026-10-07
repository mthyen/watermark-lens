package io.github.mthyen.wmlens.event;

/**
 * A periodic (roughly once-per-second) sample of element throughput, observed by
 * {@link io.github.mthyen.wmlens.api.WatermarkLens#tap}.
 *
 * @param envelope common metadata - which run, which operator/subtask, when this was recorded
 * @param count how many elements passed through during this sampling window
 * @param minEventTime the earliest event-time timestamp seen in this window
 * @param maxEventTime the latest event-time timestamp seen in this window
 * @param behindWatermark how many of those elements had a timestamp at or before the current
 *     watermark when they arrived
 */
public record EventBucketEvent(EventEnvelope envelope, long count, long minEventTime, long maxEventTime, long behindWatermark) implements WmlensEvent {
    @Override public String type() { return "event_bucket"; }
}
