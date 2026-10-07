package io.github.mthyen.wmlens.generator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Thin factory tying the pieces together, same shape as NumberSequenceSource. Source (via
 * SourceReaderFactory) extends Serializable, so this class and anything it holds must be too.
 *
 */
public class GeneratorSource
        implements Source<GeneratedEvent, GeneratorSplit, Collection<GeneratorSplit>> {

    private final GeneratorConfig generatorConfig;

    public GeneratorSource(GeneratorConfig generatorConfig) {
        this.generatorConfig = generatorConfig;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<GeneratedEvent, GeneratorSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        SplitFetcherManager<GeneratedEvent, GeneratorSplit> splitFetcherManager =
            new SingleThreadFetcherManager<GeneratedEvent, GeneratorSplit>(() -> new GeneratorSplitReader(
                generatorConfig.intervalMs(),
                generatorConfig.maxDisorderMs(),
                generatorConfig.lateness(),
                generatorConfig.idlePartition()
            ));
        RecordEmitter<GeneratedEvent, GeneratedEvent, GeneratorSplit> recordEmitter = new GeneratorRecordEmitter();
        Configuration config = new Configuration();

        return new GeneratorSourceReader(splitFetcherManager, recordEmitter, config,  readerContext, this.generatorConfig.ratePerSecond());
    }

    @Override
    public SplitEnumerator<GeneratorSplit, Collection<GeneratorSplit>> createEnumerator(
            SplitEnumeratorContext<GeneratorSplit> enumContext) throws Exception {
        List<GeneratorSplit> enumerators = new ArrayList<>();

        for (int i = 0; i < this.generatorConfig.numPartitions(); i++) {
            enumerators.add(new GeneratorSplit("partition-" + i, 0L));
        }
        return new GeneratorSplitEnumerator(enumContext, enumerators);
    }

    @Override
    public SplitEnumerator<GeneratorSplit, Collection<GeneratorSplit>> restoreEnumerator(
            SplitEnumeratorContext<GeneratorSplit> enumContext, Collection<GeneratorSplit> checkpoint)
            throws Exception {
        return new GeneratorSplitEnumerator(enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<GeneratorSplit> getSplitSerializer() {
        return new GeneratorSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<GeneratorSplit>> getEnumeratorCheckpointSerializer() {
        return new GeneratorEnumeratorCheckpointSerializer();
    }
}
