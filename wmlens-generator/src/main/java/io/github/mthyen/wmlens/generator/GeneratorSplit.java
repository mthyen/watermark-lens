package io.github.mthyen.wmlens.generator;

import org.apache.flink.api.connector.source.SourceSplit;

/**
 * A unit of work handed out by {@link GeneratorSplitEnumerator} to a {@link GeneratorSourceReader}
 * - one per simulated partition (see {@link GeneratorConfig#numPartitions()}).
 *
 * Also reused as the SplitStateT for {@link GeneratorSourceReader} / {@link GeneratorRecordEmitter}
 * for now; introduce a separate mutable state type later if resuming mid-split needs finer-grained
 * position tracking than "which split" (see NumberSequenceSource.NumberSequenceSplit /
 * getUpdatedSplitForIterator for the pattern this would follow).
 */
public class GeneratorSplit implements SourceSplit {
    private final String splitId;
    private long nextSequence;

    public GeneratorSplit(String splitId, long nextSequence) {
        this.splitId = splitId;
        this.nextSequence = nextSequence;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public long nextSequence() {
        return nextSequence;
    }

    public void setNextSequence(long nextSequence) {
        if (nextSequence < this.nextSequence) {
            throw new IllegalArgumentException(
                    "nextSequence must not move backwards: was " + this.nextSequence
                            + ", got " + nextSequence);
        }
        this.nextSequence = nextSequence;
    }

    @Override
    public String toString() {
        return "GeneratorSplit [splitId=" + splitId + ", nextSequence=" + nextSequence + "]";
    }
}
