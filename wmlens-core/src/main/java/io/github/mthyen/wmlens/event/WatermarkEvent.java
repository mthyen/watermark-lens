package io.github.mthyen.wmlens.event;

/**
 * A watermark advance observed by {@link io.github.mthyen.wmlens.api.WatermarkLens#tap}.
 *
 * @param envelope common metadata - which run, which operator/subtask, when this was recorded
 * @param watermark the new watermark value
 */
public record WatermarkEvent(EventEnvelope envelope, long watermark) implements WmlensEvent {
    @Override public String type() { return "watermark"; }
}
