package io.github.mthyen.wmlens.api;

import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.operator.LateDataTapOperator;
import io.github.mthyen.wmlens.operator.WatermarkTapOperator;
import io.github.mthyen.wmlens.sink.WmlensEventJsonEncoder;
import io.github.mthyen.wmlens.window.InstrumentedProcessWindowFunction;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.UUID;

/**
 * Entry point for instrumenting an existing Flink pipeline. Each method wraps one part of a
 * pipeline - a stream, a window function, or a late-data side output - and adds a side output of
 * {@link WmlensEvent}s describing what that part is actually doing: watermark advances, window
 * firings, elements dropped for lateness. The wrapped pipeline's own output doesn't change.
 * Union the side outputs you care about and send them to {@link #jsonlSink} to capture a run.
 */
public final class WatermarkLens {
    private WatermarkLens() {}

    /**
     * Observes watermark advances and element throughput on {@code stream} without changing what
     * it emits. Emits a {@link io.github.mthyen.wmlens.event.WatermarkEvent} for every distinct
     * watermark advance, and a periodic {@link io.github.mthyen.wmlens.event.EventBucketEvent}
     * roughly once a second, to the side output tagged {@link WmlensEvent#EVENTS_TAG}.
     */
    public static <T> SingleOutputStreamOperator<T> tap(DataStream<T> stream, WatermarkLensConfig config) {
        String operatorName = config.operatorName() != null ? config.operatorName() : "watermark-tap";
        String runId = UUID.randomUUID().toString();
        return stream.transform(operatorName, stream.getType(), new WatermarkTapOperator<>(operatorName, runId));
    }

    /**
     * Wraps {@code fn} to report each window firing, without changing what {@code fn} produces.
     * Pass the result to {@code WindowedStream.process(...)} in place of {@code fn}. Emits one
     * {@link io.github.mthyen.wmlens.event.WindowFiredEvent} per firing - distinguishing a
     * window's first firing from a late element re-firing it within {@code allowedLateness} - to
     * the side output tagged {@link WmlensEvent#EVENTS_TAG}.
     */
    public static <IN, OUT, KEY, W extends TimeWindow> ProcessWindowFunction<IN, OUT, KEY, W> instrument(
        ProcessWindowFunction<IN, OUT, KEY, W> fn, WatermarkLensConfig config) {
        String operatorName = config.operatorName() != null ? config.operatorName() : "watermark-window";
        String runId = UUID.randomUUID().toString();
        return new InstrumentedProcessWindowFunction<>(fn, operatorName, runId);
    }

    /**
     * Observes elements that arrived too late to be included in their window (those routed to a
     * side output via {@code WindowedStream.sideOutputLateData(tag)}), emitting a {@code
     * LateDropEvent} for each one without altering the stream's contents.
     *
     * <p>The caller must re-key the late-data stream with the <b>same</b> {@code KeySelector} used
     * in the original {@code keyBy(...)} before windowing. This operator reports each event's key via
     * {@code getCurrentKey()}, which only reflects the key Flink actually partitioned by - a
     * different (even differently-implemented-but-equivalent) selector will silently produce a
     * stream that's keyed, just not the way the caller thinks, with no error at compile time or
     * runtime to catch the mismatch.
     */
    public static <T, KEY> SingleOutputStreamOperator<T> observeLateData(KeyedStream<T, KEY> stream, WatermarkLensConfig config) {
        String operatorName = config.operatorName() != null ? config.operatorName() : "watermark-late-data";
        String runId = UUID.randomUUID().toString();
        return stream.transform(operatorName, stream.getType(), new LateDataTapOperator<>(operatorName, runId));
    }

    /**
     * A {@link Sink} for the {@link WmlensEvent} side outputs produced by {@link #tap}, {@link
     * #instrument}, and {@link #observeLateData}, writing one JSON object per line under {@code
     * basePath}. Rolls - and so becomes visible on disk - on every checkpoint, so the job must
     * have checkpointing enabled or files never finalize.
     */
    public static Sink<WmlensEvent> jsonlSink(String basePath) {
        return FileSink.forRowFormat(new Path(basePath), new WmlensEventJsonEncoder())
            .withBucketAssigner(new BasePathBucketAssigner<>())
            // Roll (and thus commit/finalize) on every checkpoint rather than DefaultRollingPolicy's
            // 60s-by-default size/time thresholds - this is a low-volume diagnostic stream, so
            // frequent small files that become visible quickly matters more than fewer large ones.
            .withRollingPolicy(org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy.build())
            .build();
    }
}
