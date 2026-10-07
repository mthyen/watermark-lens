package io.github.mthyen.wmlens.operator;

import io.github.mthyen.wmlens.event.EventBucketEvent;
import io.github.mthyen.wmlens.event.EventEnvelope;
import io.github.mthyen.wmlens.event.WatermarkEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Observes watermark advances and element arrivals on an existing pipeline without changing what
 * it outputs. Extends AbstractStreamOperator (not ProcessFunction) because processWatermark is
 * the only hook that sees every watermark advance - see the package-info for why.
 *
 * All state below is plain instance fields, not checkpointed via CheckpointedFunction/ListState -
 * a deliberate simplification for now, since this is diagnostic data where losing a few seconds
 * of it on restart is acceptable. Revisit if that ever matters.
 */
public class WatermarkTapOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T> {

    public static final long FLUSH_INTERVAL_MS = 1000L;

    private final String operator;
    private final String runId;

    // Most recent watermark seen. Does double duty: change-detection in processWatermark, and
    // the behind-the-watermark comparison in processElement.
    private long currentWatermark = Watermark.UNINITIALIZED.getTimestamp();

    // Per-bucket accumulators for the event_bucket record (count/minEventTime/maxEventTime/
    // behindWatermark); reset after every flush in flushBucket() below.
    private long bucketCount = 0;
    private long bucketMinEventTime = Long.MAX_VALUE;
    private long bucketMaxEventTime = Long.MIN_VALUE;
    private long bucketBehindWatermarkCount = 0;


    public WatermarkTapOperator(String operator, String runId) {
        this.operator = operator;
        this.runId = runId;
    }

    @Override
    public void open() throws Exception {
        super.open();
        getProcessingTimeService().scheduleAtFixedRate(this::flushBucket, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS);
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        long timestamp = element.getTimestamp();
        this.bucketMinEventTime = Math.min(this.bucketMinEventTime, timestamp);
        this.bucketMaxEventTime = Math.max(this.bucketMaxEventTime, timestamp);
        if (timestamp <= this.currentWatermark) {
            this.bucketBehindWatermarkCount++;
        }

        this.bucketCount++;
        output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.getTimestamp() != this.currentWatermark) {
            long now = System.currentTimeMillis();
            EventEnvelope envelope = createEventEnvelope(now);

            WatermarkEvent event = new WatermarkEvent(envelope, mark.getTimestamp());

            output.collect(WmlensEvent.EVENTS_TAG, new StreamRecord<>(event, now));
            this.currentWatermark = mark.getTimestamp();
        }

        super.processWatermark(mark);
    }

    private void flushBucket(long time) throws Exception {
        if (this.bucketCount > 0) {
            long now = System.currentTimeMillis();
            EventEnvelope envelope = createEventEnvelope(now);
            EventBucketEvent event = new EventBucketEvent(envelope, this.bucketCount, this.bucketMinEventTime, this.bucketMaxEventTime, this.bucketBehindWatermarkCount);
            output.collect(WmlensEvent.EVENTS_TAG, new StreamRecord<>(event, now));
        }

        // reset everything regardless of bucketCount
        bucketCount = 0;
        bucketMinEventTime = Long.MAX_VALUE;
        bucketMaxEventTime = Long.MIN_VALUE;
        bucketBehindWatermarkCount = 0;
    }

    private EventEnvelope createEventEnvelope(long now) {
        return new EventEnvelope(
            EventEnvelope.CURRENT_VERSION,
            this.runId,
            now,
            this.operator,
            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
        );
    }
}
