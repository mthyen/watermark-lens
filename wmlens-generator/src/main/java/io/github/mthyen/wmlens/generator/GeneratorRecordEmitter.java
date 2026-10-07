package io.github.mthyen.wmlens.generator;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * Converts what the split reader produced into what actually gets emitted downstream, and
 * assigns the event timestamp via SourceOutput (not a plain Collector - this is how the
 * timestamp that WatermarkStrategy sees gets set).
 */
public class GeneratorRecordEmitter
        implements RecordEmitter<GeneratedEvent, GeneratedEvent, GeneratorSplit> {

    @Override
    public void emitRecord(
            GeneratedEvent element, SourceOutput<GeneratedEvent> output, GeneratorSplit splitState)
            throws Exception {
        output.collect(element, element.eventTime());
        splitState.setNextSequence(element.sequence() + 1);
    }
}
