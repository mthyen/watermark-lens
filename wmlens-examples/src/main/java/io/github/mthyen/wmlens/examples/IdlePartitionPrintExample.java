package io.github.mthyen.wmlens.examples;

import io.github.mthyen.wmlens.generator.GeneratedEvent;
import io.github.mthyen.wmlens.generator.GeneratorConfig;
import io.github.mthyen.wmlens.generator.GeneratorSource;
import io.github.mthyen.wmlens.generator.IdlePartitionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class IdlePartitionPrintExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        WatermarkStrategy<GeneratedEvent> watermarkStrategy = WatermarkStrategy.<GeneratedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2));

        DataStreamSource<GeneratedEvent> stream = env.fromSource(
            new GeneratorSource(
                new GeneratorConfig(
                    2,
                    1000L,
                    1.0,
                    0,
                    null,
                    new IdlePartitionConfig("partition-0", 3, 5)
                )
            ),
            watermarkStrategy,
            "generator"
        );

        stream.print();
        env.execute("Idle Partition Print Example");
    }
}
