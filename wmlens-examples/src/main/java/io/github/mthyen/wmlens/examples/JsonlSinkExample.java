package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class JsonlSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Single-partition generator, so one subtask is enough for this simple example.
        env.setParallelism(1);
        // Required for FileSink to ever roll/finalize visible files - see jsonlSink()'s own
        // Javadoc for why it rolls on every checkpoint rather than FileSink's size/time defaults.
        env.enableCheckpointing(2000);
        WatermarkStrategy<GeneratedEvent> watermarkStrategy = WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

        DataStreamSource<GeneratedEvent> stream = env.fromSource(
            new GeneratorSource(
                new GeneratorConfig(1, 1000L, 1.0, 0, null, null)),
            watermarkStrategy,
            "generator"
        );

        SingleOutputStreamOperator<GeneratedEvent> tapped = WatermarkLens.tap(stream, new WatermarkLensConfig("generator-tap"));
        tapped.print("main");

        DataStream<WmlensEvent> events = tapped.getSideOutput(WmlensEvent.EVENTS_TAG);
        events.sinkTo(WatermarkLens.jsonlSink("/tmp/wmlens-events"));

        env.execute("JSONL Sink Example");
    }
}
