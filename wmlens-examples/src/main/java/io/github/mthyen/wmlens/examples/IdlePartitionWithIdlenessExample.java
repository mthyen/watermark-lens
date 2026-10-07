package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import io.github.mthyen.wmlens.generator.IdlePartitionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Same pipeline as {@link IdlePartitionJsonlExample}, with one line added:
 * {@code withIdleness(...)} on the watermark strategy. That should mark partition-0's channel
 * idle after the configured timeout, which excludes it from the downstream min-watermark
 * computation and lets the merged watermark keep advancing off the active partition alone -
 * compare the two sink outputs to see the difference {@code withIdleness} makes.
 */
public class IdlePartitionWithIdlenessExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2000);

        WatermarkStrategy<GeneratedEvent> watermarkStrategy =
                WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withIdleness(Duration.ofMillis(500));

        DataStreamSource<GeneratedEvent> source = env.fromSource(
                new GeneratorSource(new GeneratorConfig(
                        2, 1000L, 1.0, 0, null, new IdlePartitionConfig("partition-0", 3, 5))),
                watermarkStrategy,
                "generator");

        KeyedStream<GeneratedEvent, String> shuffled = source.keyBy(e -> "k");
        SingleOutputStreamOperator<GeneratedEvent> tapped =
                WatermarkLens.tap(shuffled, new WatermarkLensConfig("merged-watermark-tap"));
        tapped.print();

        DataStream<WmlensEvent> events = tapped.getSideOutput(WmlensEvent.EVENTS_TAG);
        events.sinkTo(WatermarkLens.jsonlSink("/tmp/wmlens-idle-with-idleness"));

        env.execute("Idle Partition JSONL Example (with withIdleness)");
    }
}
