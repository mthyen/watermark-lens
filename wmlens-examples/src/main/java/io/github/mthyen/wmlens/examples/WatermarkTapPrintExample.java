package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkTapPrintExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<GeneratedEvent> watermarkStrategy = WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

        DataStreamSource<GeneratedEvent> stream = env.fromSource(
            new GeneratorSource(
                new GeneratorConfig(1, 1000L, 1.0, 0, null, null)
            ),
            watermarkStrategy,
            "generator");

        SingleOutputStreamOperator<GeneratedEvent> tapped = WatermarkLens.tap(stream, new WatermarkLensConfig("generator-tap"));
        tapped.print("main");
        tapped.getSideOutput(WmlensEvent.EVENTS_TAG).print("events");

        env.execute("Watermark Tap Print Example");
    }
}
