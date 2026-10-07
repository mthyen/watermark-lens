package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class GeneratorPrintExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WatermarkStrategy<GeneratedEvent> watermarkStrategy = WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

        DataStreamSource<GeneratedEvent> stream = env.fromSource(
            new GeneratorSource(
                new GeneratorConfig(1, 1000L, 1.0, 0, null, null)),
            watermarkStrategy,
            "generator"
        );

        stream.print();
        env.execute("Generator Print Example");
    }
}
