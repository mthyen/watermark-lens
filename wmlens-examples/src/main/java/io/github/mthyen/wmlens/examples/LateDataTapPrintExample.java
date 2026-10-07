package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import io.github.mthyen.wmlens.generator.LatenessConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataTapPrintExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<GeneratedEvent> watermarkStrategy = WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

        OutputTag<GeneratedEvent> lateTag = new OutputTag<>("late-data") {};
        SingleOutputStreamOperator<GeneratedEvent> stream = env.fromSource(
            new GeneratorSource(
                new GeneratorConfig(1, 1000L, 1.0, 0,  new LatenessConfig(0.5, 6000, 10000), null)
            ),
            watermarkStrategy,
            "generator"
        ).keyBy(e -> "k")
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .allowedLateness(Duration.ZERO)
            .sideOutputLateData(lateTag)
            .reduce((a, b) -> a);

        stream.print("windowed");

        KeyedStream<GeneratedEvent, String> keyedLateStream = stream
            .getSideOutput(lateTag)
            .keyBy(e -> "k");

        SingleOutputStreamOperator<GeneratedEvent> lateTapped = WatermarkLens.observeLateData(keyedLateStream, new WatermarkLensConfig("late-data-tap"));
        lateTapped.print("late-main");
        lateTapped.getSideOutput(WmlensEvent.EVENTS_TAG).print("late-events");

        env.execute("LateData Tap Print Example");
    }
}
