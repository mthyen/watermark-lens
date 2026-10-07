package io.github.mthyen.wmlens.event;

/**
 * Metadata attached to every {@link WmlensEvent}, identifying where and when it was recorded.
 *
 * @param v the envelope schema version, for forward compatibility as the event model evolves
 * @param runId a random id generated once per {@code WatermarkLens} call, shared by every event
 *     that operator instance ever emits - use it to tell two runs of the same job apart
 * @param wallTime the wall-clock time this event was recorded (not event time), in epoch millis
 * @param operator the operator name from {@link io.github.mthyen.wmlens.api.WatermarkLensConfig}
 * @param subtask which parallel subtask recorded this event
 */
public record EventEnvelope(int v, String runId, long wallTime, String operator, int subtask) {
    public static final int CURRENT_VERSION = 1;
}
