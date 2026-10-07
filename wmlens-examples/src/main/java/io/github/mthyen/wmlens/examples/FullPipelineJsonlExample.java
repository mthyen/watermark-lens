package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import io.github.mthyen.wmlens.generator.LatenessConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Exercises tap(), instrument(), and observeLateData() together in one pipeline, all feeding a
 * single jsonlSink() - demonstrates the full diagnostic event model (watermark, event_bucket,
 * window_fired with both ON_TIME and LATE_UPDATE firings, and late_drop) in one output file.
 */
public class FullPipelineJsonlExample {

    private static class CountWindowFunction
            extends ProcessWindowFunction<GeneratedEvent, Long, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<GeneratedEvent> elements, Collector<Long> out) {
            long count = 0;
            for (GeneratedEvent ignored : elements) {
                count++;
            }
            out.collect(count);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000);

        WatermarkStrategy<GeneratedEvent> watermarkStrategy =
                WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));
        DataStreamSource<GeneratedEvent> source = env.fromSource(
                new GeneratorSource(new GeneratorConfig(
                        1, 1000L, 1.0, 0, new LatenessConfig(0.6, 1000, 15000), null)),
                watermarkStrategy,
                "generator");

        SingleOutputStreamOperator<GeneratedEvent> tapped =
                WatermarkLens.tap(source, new WatermarkLensConfig("generator-tap"));

        OutputTag<GeneratedEvent> lateTag = new OutputTag<GeneratedEvent>("late-data") {};
        SingleOutputStreamOperator<Long> windowed = tapped.keyBy(e -> "k")
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .allowedLateness(Duration.ofSeconds(3))
                .sideOutputLateData(lateTag)
                .process(WatermarkLens.instrument(new CountWindowFunction(), new WatermarkLensConfig("window-instrument")))
                .returns(org.apache.flink.api.common.typeinfo.Types.LONG);

        KeyedStream<GeneratedEvent, String> keyedLate = windowed.getSideOutput(lateTag).keyBy(e -> "k");
        SingleOutputStreamOperator<GeneratedEvent> lateTapped =
                WatermarkLens.observeLateData(keyedLate, new WatermarkLensConfig("late-data-tap"));

        DataStream<WmlensEvent> allEvents = tapped.getSideOutput(WmlensEvent.EVENTS_TAG)
                .union(windowed.getSideOutput(WmlensEvent.EVENTS_TAG))
                .union(lateTapped.getSideOutput(WmlensEvent.EVENTS_TAG));

        allEvents.sinkTo(WatermarkLens.jsonlSink("/tmp/wmlens-full-sample"));

        env.execute("Full Pipeline JSONL Example");
    }
}
