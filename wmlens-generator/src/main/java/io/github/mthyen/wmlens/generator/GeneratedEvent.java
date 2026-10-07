package io.github.mthyen.wmlens.generator;

/**
 * A single synthetic event produced by the generator.
 *
 * @param eventTime the event's timestamp, including any lateness/disorder offset already applied
 * @param sequence this event's position in its partition's sequence - unaffected by lateness or
 *     disorder, so it reflects generation order, not event-time order
 * @param lateOffsetMs how much this event's timestamp was pushed behind "now" to simulate
 *     lateness, or 0 if it wasn't made late
 * @param disorderOffsetMs the random jitter applied to this event's timestamp to simulate
 *     disorder, or 0 if disorder is disabled
 */
public record GeneratedEvent(long eventTime, long sequence, long lateOffsetMs, long disorderOffsetMs) {
}
