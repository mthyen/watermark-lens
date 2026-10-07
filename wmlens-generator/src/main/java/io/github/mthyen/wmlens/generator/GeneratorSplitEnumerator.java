package io.github.mthyen.wmlens.generator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * Runs as a single instance on the JobManager. All splits are known upfront (no dynamic
 * discovery), so this just hands out whatever splits it was given at construction time as
 * subtasks request them.
 */
public class GeneratorSplitEnumerator
        implements SplitEnumerator<GeneratorSplit, Collection<GeneratorSplit>> {

    private SplitEnumeratorContext<GeneratorSplit> context;
    private Queue<GeneratorSplit> remainingSplits;

    public GeneratorSplitEnumerator(
            SplitEnumeratorContext<GeneratorSplit> context, Collection<GeneratorSplit> splits) {

        this.context = context;
        remainingSplits = new ArrayDeque<>(splits);
    }

    @Override
    public void start() {
        // no-op by design. All splits are known up front. No dynamic discovery
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        final GeneratorSplit next = remainingSplits.poll();
        if (next != null) {
            context.assignSplit(next, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<GeneratorSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // no-op by design. Assignment happens through handleSplitRequest when the reader asks, not eagerly when it registers
    }

    @Override
    public Collection<GeneratorSplit> snapshotState(long checkpointId) throws Exception {
        return remainingSplits;
    }

    @Override
    public void close() throws IOException {
        // no-op by design. Nothing allocated so nothing to close
    }
}
