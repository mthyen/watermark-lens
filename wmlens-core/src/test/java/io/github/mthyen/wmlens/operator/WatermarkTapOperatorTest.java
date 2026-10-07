package io.github.mthyen.wmlens.operator;

import io.github.mthyen.wmlens.event.EventBucketEvent;
import io.github.mthyen.wmlens.event.WatermarkEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WatermarkTapOperatorTest {

    private OneInputStreamOperatorTestHarness<Long, Long> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new OneInputStreamOperatorTestHarness<>(new WatermarkTapOperator<>("watermark-tap", "run-1"));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private List<WmlensEvent> sideOutputEvents() {
        List<WmlensEvent> events = new ArrayList<>();
        for (StreamRecord<WmlensEvent> record : harness.getSideOutput(WmlensEvent.EVENTS_TAG)) {
            events.add(record.getValue());
        }
        return events;
    }

    @Test
    void processElement_passesTheElementThroughUnchanged() throws Exception {
        harness.processElement(42L, 1_000L);

        List<StreamRecord<? extends Long>> output = harness.extractOutputStreamRecords();

        assertEquals(1, output.size());
        assertEquals(42L, output.get(0).getValue());
        assertEquals(1_000L, output.get(0).getTimestamp());
    }

    @Test
    void processWatermark_emitsAWatermarkEventWithTheConfiguredOperatorAndRunId() throws Exception {
        harness.processWatermark(500L);

        List<WmlensEvent> events = sideOutputEvents();

        assertEquals(1, events.size());
        assertTrue(events.get(0) instanceof WatermarkEvent);
        WatermarkEvent event = (WatermarkEvent) events.get(0);
        assertEquals(500L, event.watermark());
        assertEquals("watermark-tap", event.envelope().operator());
        assertEquals("run-1", event.envelope().runId());
        assertEquals(0, event.envelope().subtask());
    }

    @Test
    void processWatermark_doesNotEmitAgainForARepeatedSameValue() throws Exception {
        harness.processWatermark(500L);
        harness.processWatermark(500L);

        assertEquals(1, sideOutputEvents().size(),
                "a watermark that hasn't actually advanced shouldn't produce a second event");
    }

    @Test
    void processWatermark_emitsOnceForEachDistinctAdvance() throws Exception {
        harness.processWatermark(100L);
        harness.processWatermark(200L);
        harness.processWatermark(300L);

        List<Long> watermarks = sideOutputEvents().stream()
                .map(e -> ((WatermarkEvent) e).watermark())
                .toList();

        assertEquals(List.of(100L, 200L, 300L), watermarks);
    }

    @Test
    void flushBucket_onTimer_emitsCountsAndRangeForElementsSeenSinceTheLastFlush() throws Exception {
        harness.processWatermark(150L);
        harness.processElement(1L, 100L); // behind the watermark above
        harness.processElement(2L, 300L);
        harness.processElement(3L, 200L);

        harness.setProcessingTime(WatermarkTapOperator.FLUSH_INTERVAL_MS);

        List<EventBucketEvent> buckets = sideOutputEvents().stream()
                .filter(e -> e instanceof EventBucketEvent)
                .map(e -> (EventBucketEvent) e)
                .toList();

        assertEquals(1, buckets.size());
        EventBucketEvent bucket = buckets.get(0);
        assertEquals(3L, bucket.count());
        assertEquals(100L, bucket.minEventTime());
        assertEquals(300L, bucket.maxEventTime());
        assertEquals(1L, bucket.behindWatermark());
    }

    @Test
    void flushBucket_withNoElementsSinceTheLastFlush_emitsNothing() throws Exception {
        harness.processElement(1L, 100L);
        harness.setProcessingTime(WatermarkTapOperator.FLUSH_INTERVAL_MS); // first flush: 1 element

        harness.setProcessingTime(2 * WatermarkTapOperator.FLUSH_INTERVAL_MS); // second flush: none

        long bucketEvents = sideOutputEvents().stream().filter(e -> e instanceof EventBucketEvent).count();
        assertEquals(1, bucketEvents, "an empty flush interval shouldn't emit an empty bucket event");
    }

    @Test
    void flushBucket_resetsAccumulatorsAfterEachFlush() throws Exception {
        harness.processElement(1L, 100L);
        harness.setProcessingTime(WatermarkTapOperator.FLUSH_INTERVAL_MS);

        harness.processElement(2L, 500L);
        harness.setProcessingTime(2 * WatermarkTapOperator.FLUSH_INTERVAL_MS);

        List<EventBucketEvent> buckets = sideOutputEvents().stream()
                .filter(e -> e instanceof EventBucketEvent)
                .map(e -> (EventBucketEvent) e)
                .toList();

        assertEquals(2, buckets.size());
        assertEquals(1L, buckets.get(0).count());
        assertEquals(1L, buckets.get(1).count());
        assertEquals(500L, buckets.get(1).minEventTime(),
                "the second bucket shouldn't still include the first bucket's element");
    }
}
