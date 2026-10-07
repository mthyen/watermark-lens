package io.github.mthyen.wmlens.event;

/**
 * An element dropped for arriving too late, observed by
 * {@link io.github.mthyen.wmlens.api.WatermarkLens#observeLateData}.
 *
 * @param envelope common metadata - which run, which operator/subtask, when this was recorded
 * @param key the element's key, as {@code String.valueOf(key)} - see {@link
 *     io.github.mthyen.wmlens.api.WatermarkLens#observeLateData} for why this depends on the
 *     caller re-keying with the same {@code KeySelector} used for windowing
 * @param eventTime the dropped element's own event-time timestamp
 * @param watermarkAtArrival the watermark at the moment the element arrived
 * @param latenessMs how far behind the watermark the element was ({@code watermarkAtArrival -
 *     eventTime})
 */
public record LateDropEvent(
    EventEnvelope envelope,
    String key,
    long eventTime,
    long watermarkAtArrival,
    long latenessMs
) implements WmlensEvent {
    @Override public String type() { return "late_drop"; }
}