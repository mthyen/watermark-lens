package io.github.mthyen.wmlens.operator;

import io.github.mthyen.wmlens.event.EventEnvelope;
import io.github.mthyen.wmlens.event.LateDropEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Relies on the caller using the same {@code KeySelector} as the original {@code keyBy(...)} -
 * see {@link io.github.mthyen.wmlens.api.WatermarkLens#observeLateData} for why.
 */
public class LateDataTapOperator<T> extends AbstractStreamOperator<T>
    implements OneInputStreamOperator<T, T> {

    private final String operator;
    private final String runId;

    private long currentWatermark = Watermark.UNINITIALIZED.getTimestamp();

    public LateDataTapOperator(String operator, String runId) {
        this.operator = operator;
        this.runId = runId;
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {

        long eventTime = element.getTimestamp();
        String key = String.valueOf(getCurrentKey());
        long watermarkAtArrival = this.currentWatermark;
        long now = System.currentTimeMillis();

        long latenessMs = watermarkAtArrival - eventTime;
        EventEnvelope envelope = new EventEnvelope(
            EventEnvelope.CURRENT_VERSION,
            this.runId,
            now,
            this.operator,
            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()
        );

        LateDropEvent lateDropEvent = new LateDropEvent(
            envelope,
            key,
            eventTime,
            watermarkAtArrival,
            latenessMs
        );

        output.collect(WmlensEvent.EVENTS_TAG, new StreamRecord<>(lateDropEvent, now));
        // pass on original event
        output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        this.currentWatermark = mark.getTimestamp();
        super.processWatermark(mark);
    }
}
