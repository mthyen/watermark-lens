package io.github.mthyen.wmlens.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.OutputTag;

/**
 * The common type of every diagnostic event Watermark Lens produces: a
 * {@link WatermarkEvent watermark advance}, a periodic {@link EventBucketEvent throughput
 * sample}, a {@link WindowFiredEvent window firing}, or a {@link LateDropEvent late-dropped
 * element}. Every operator in this library writes to the same side output tag,
 * {@link #EVENTS_TAG}, so a pipeline using several of them can union all the side outputs into
 * one stream and send it to {@link io.github.mthyen.wmlens.api.WatermarkLens#jsonlSink}.
 */
public sealed interface WmlensEvent permits WatermarkEvent, EventBucketEvent, WindowFiredEvent, LateDropEvent {
    /** Side output tag every {@code WmlensEvent}-producing operator writes to. */
    public static final OutputTag<WmlensEvent> EVENTS_TAG = new OutputTag<WmlensEvent>("wmlens-events") {};

    EventEnvelope envelope();

    @JsonProperty("type")
    String type();
}