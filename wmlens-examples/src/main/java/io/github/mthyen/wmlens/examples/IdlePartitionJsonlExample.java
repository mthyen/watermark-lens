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
 * Milestone 5, experiment 2: {@code IdlePartitionPrintExample} already demonstrates
 * {@code IdlePartitionConfig} but only via raw {@code print()} right off the source - it never
 * shows what an idle partition does to a <em>combined</em> watermark. Here, {@code keyBy} forces
 * a shuffle (a hash/all-to-all exchange) before the tap, so every downstream subtask has an input
 * channel from every upstream partition, including the idle one - that's what makes Flink's
 * per-operator watermark the min across all of them. Uses a plain
 * {@code forBoundedOutOfOrderness} strategy (no {@code withIdleness}), so partition-0 going idle
 * is free to hold back the merged watermark seen after the shuffle. Compare against
 * {@link IdlePartitionWithIdlenessExample}, which adds {@code withIdleness} to the same pipeline.
 */
public class IdlePartitionJsonlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(2000);

        WatermarkStrategy<GeneratedEvent> watermarkStrategy =
                WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

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
        events.sinkTo(WatermarkLens.jsonlSink("/tmp/wmlens-idle-no-idleness"));

        env.execute("Idle Partition JSONL Example (no withIdleness)");
    }
}
