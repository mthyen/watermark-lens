package io.github.mthyen.wmlens.event;

/**
 * A window firing observed by {@link io.github.mthyen.wmlens.api.WatermarkLens#instrument}.
 *
 * @param envelope common metadata - which run, which operator/subtask, when this was recorded
 * @param key the window's key, as {@code String.valueOf(key)}
 * @param windowStart the window's start timestamp
 * @param windowEnd the window's end timestamp
 * @param count how many elements were in the window when it fired
 * @param watermarkAtFire the watermark at the moment this firing happened
 * @param firing whether this was the window's first firing or a late element re-firing it
 */
public record WindowFiredEvent(
    EventEnvelope envelope,
    String key,
    long windowStart,
    long windowEnd,
    long count,
    long watermarkAtFire,
    FiringType firing
) implements WmlensEvent {
    @Override public String type() { return "window_fired"; }
}
