package io.github.mthyen.wmlens.generator;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.GuavaRateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;

import java.util.Map;

/**
 * SourceReaderBase implements pollNext/snapshotState/addSplits/close for us; we only need to
 * fill in how split state is created/converted and what happens when a split finishes.
 */
public class GeneratorSourceReader
        extends SourceReaderBase<GeneratedEvent, GeneratedEvent, GeneratorSplit, GeneratorSplit> {

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    public GeneratorSourceReader(
            SplitFetcherManager<GeneratedEvent, GeneratorSplit> splitFetcherManager,
            RecordEmitter<GeneratedEvent, GeneratedEvent, GeneratorSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            double ratePerSecond) {
        super(splitFetcherManager, recordEmitter, null, config, context, parallelism -> new GuavaRateLimiter<>(ratePerSecond));
    }

    @Override
    protected GeneratorSplit initializedState(GeneratorSplit split) {
        return new GeneratorSplit(split.splitId(), split.nextSequence());
    }

    @Override
    protected GeneratorSplit toSplitType(String splitId, GeneratorSplit splitState) {
        return new GeneratorSplit(splitId, splitState.nextSequence());
    }

    @Override
    protected void onSplitFinished(Map<String, GeneratorSplit> finishedSplitIds) {
        // not reachable due to design of this to not ever finish a split
    }
}
