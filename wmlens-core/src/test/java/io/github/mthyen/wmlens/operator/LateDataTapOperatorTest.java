package io.github.mthyen.wmlens.operator;

import io.github.mthyen.wmlens.event.LateDropEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LateDataTapOperatorTest {

    private static final KeySelector<Long, String> KEY_SELECTOR = String::valueOf;

    private KeyedOneInputStreamOperatorTestHarness<String, Long, Long> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                new LateDataTapOperator<>("late-data-tap", "run-1"),
                KEY_SELECTOR,
                BasicTypeInfo.STRING_TYPE_INFO);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private List<LateDropEvent> sideOutputEvents() {
        List<LateDropEvent> events = new ArrayList<>();
        for (StreamRecord<WmlensEvent> record : harness.getSideOutput(WmlensEvent.EVENTS_TAG)) {
            events.add((LateDropEvent) record.getValue());
        }
        return events;
    }

    @Test
    void processElement_passesTheElementThroughUnchanged() throws Exception {
        harness.processElement(7L, 100L);

        List<StreamRecord<? extends Long>> output = harness.extractOutputStreamRecords();

        assertEquals(1, output.size());
        assertEquals(7L, output.get(0).getValue());
        assertEquals(100L, output.get(0).getTimestamp());
    }

    @Test
    void processElement_emitsALateDropEventKeyedByTheConfiguredKeySelector() throws Exception {
        harness.processElement(7L, 100L);

        List<LateDropEvent> events = sideOutputEvents();

        assertEquals(1, events.size());
        assertEquals("7", events.get(0).key());
        assertEquals(100L, events.get(0).eventTime());
        assertEquals("late-data-tap", events.get(0).envelope().operator());
        assertEquals("run-1", events.get(0).envelope().runId());
    }

    @Test
    void processElement_computesLatenessRelativeToTheCurrentWatermark() throws Exception {
        harness.processWatermark(1_000L);
        harness.processElement(7L, 700L);

        LateDropEvent event = sideOutputEvents().get(0);

        assertEquals(1_000L, event.watermarkAtArrival());
        assertEquals(700L, event.eventTime());
        assertEquals(300L, event.latenessMs());
    }

    @Test
    void processElement_beforeAnyWatermark_usesTheUninitializedWatermarkValue() throws Exception {
        harness.processElement(7L, 700L);

        LateDropEvent event = sideOutputEvents().get(0);

        assertEquals(Watermark.UNINITIALIZED.getTimestamp(), event.watermarkAtArrival());
    }

    @Test
    void processElement_usesTheKeyActuallyRoutedForEachElement_notAStaleOne() throws Exception {
        harness.processElement(1L, 100L);
        harness.processElement(2L, 200L);

        List<String> keys = sideOutputEvents().stream().map(LateDropEvent::key).toList();

        assertTrue(keys.contains("1") && keys.contains("2"),
                "expected each event's key to reflect its own element, got: " + keys);
    }
}
