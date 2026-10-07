package io.github.mthyen.wmlens.generator;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.LongSupplier;

/**
 * Runs on its own fetcher thread (not the mailbox thread), so fetch() is free to block/pace
 * itself. This is where the actual event generation logic lives.
 *
 * Note: SplitReader also has a default pauseOrResumeSplits(...) method not overridden here -
 * see the interface if/when that becomes relevant.
 */
public class GeneratorSplitReader implements SplitReader<GeneratedEvent, GeneratorSplit> {
    private final long intervalMs;
    private final long maxDisorderMs;
    private final LatenessConfig lateness;
    private final IdlePartitionConfig idlePartition;
    private final LongSupplier clock;
    private final long baseTime;

    private final Map<String, GeneratorSplit> splits = new HashMap<>();
    private final Random random = new Random();

    private long nextTickTime;

    private volatile Thread fetcherThread;

    public GeneratorSplitReader(
        long intervalMs, long maxDisorderMs, LatenessConfig lateness, IdlePartitionConfig idlePartition
    ) {
        this(intervalMs, maxDisorderMs, lateness, idlePartition, System::currentTimeMillis);
    }

    /**
     * Package-private: lets tests fake the passage of time instead of actually blocking through
     * fetch()'s real pacing (see GeneratorSplitReaderTest's FakeClock) - production code always
     * goes through the public constructor above, which wires up the real wall clock.
     */
    GeneratorSplitReader(
        long intervalMs, long maxDisorderMs, LatenessConfig lateness, IdlePartitionConfig idlePartition,
        LongSupplier clock
    ) {
        this.intervalMs = intervalMs;
        this.maxDisorderMs = maxDisorderMs;
        this.lateness = lateness;
        this.idlePartition = idlePartition;
        this.clock = clock;
        this.baseTime = clock.getAsLong();
        this.nextTickTime = this.baseTime;
    }

    @Override
    public RecordsWithSplitIds<GeneratedEvent> fetch() throws IOException {
        long now = clock.getAsLong();
        this.fetcherThread = Thread.currentThread();

        if (now < nextTickTime) {
            try {
                Thread.sleep(nextTickTime - now);
            } catch (InterruptedException e) {
                return new RecordsBySplits.Builder<GeneratedEvent>().build();
            }
        }
        long eventTime;
        RecordsBySplits.Builder<GeneratedEvent> builder = new RecordsBySplits.Builder<>();

        for (Map.Entry<String, GeneratorSplit> entry : splits.entrySet()) {
            // idle partition set up
            String splitId = entry.getKey();
            GeneratorSplit split = entry.getValue();
            long nextSequence = split.nextSequence();

            if (this.idlePartition != null) {
                if (splitId.equals(this.idlePartition.idlePartitionId()) &&
                    (this.idlePartition.idleAfterSequence() <= nextSequence &&
                        nextSequence < this.idlePartition.idleAfterSequence() + this.idlePartition.idleSequenceCount()
                    )
                ) {
                    split.setNextSequence(nextSequence + 1);
                    continue;  // skip this intentionally for idle partitions
                }
            }

            // disorder set up
            long disorderOffsetMs = maxDisorderMs == 0 ? 0L : random.nextLong(-this.maxDisorderMs, this.maxDisorderMs + 1);

            // setup for random lateness if we are using it
            boolean isLate = this.lateness != null && this.lateness.lateFraction() != 0 && random.nextDouble() < this.lateness.lateFraction();
            long offsetMs = 0L;

            if (isLate && this.lateness != null) {
                if (this.lateness.maxLateOffsetMs() == Long.MAX_VALUE) {
                    do {
                        offsetMs = random.nextLong();
                    } while (offsetMs < this.lateness.minLateOffsetMs());
                } else {
                    offsetMs = random.nextLong(this.lateness.minLateOffsetMs(), this.lateness.maxLateOffsetMs() + 1);
                }
            }

            eventTime = baseTime + (nextSequence * intervalMs) - offsetMs + disorderOffsetMs;

            builder.add(splitId, new GeneratedEvent(eventTime, nextSequence, offsetMs, disorderOffsetMs));
            split.setNextSequence(nextSequence + 1);
        }
        nextTickTime += intervalMs;
        return builder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<GeneratorSplit> splitsChanges) {
        if (splitsChanges instanceof SplitsAddition<GeneratorSplit> addition) {
            for (GeneratorSplit split : addition.splits()) {
                splits.put(split.splitId(), split);
            }
        }  else {
            throw new UnsupportedOperationException("Unsupported class for splitsChange: " + splitsChanges.getClass());
        }
    }

    @Override
    public void wakeUp() {
        Thread t = fetcherThread;
        if (t != null) {
            t.interrupt();
        }
    }

    @Override
    public void close() throws Exception {
        // No-op because we do not open anything that needs to be closed
    }
}
