package io.github.mthyen.wmlens.api;

/**
 * Per-tap configuration for {@link WatermarkLens}.
 *
 * @param operatorName name shown in the Flink UI for this tap's operator, and recorded on every
 *     event it emits (see {@link io.github.mthyen.wmlens.event.EventEnvelope#operator()}). Each
 *     {@code WatermarkLens} method falls back to its own default name if this is {@code null}.
 */
public record WatermarkLensConfig(String operatorName) {
}
