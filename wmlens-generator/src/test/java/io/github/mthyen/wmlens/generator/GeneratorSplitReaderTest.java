package io.github.mthyen.wmlens.generator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorSplitReaderTest {

    private static final String SPLIT_ID = "partition-0";
    private static final long INTERVAL_MS = 1000L;

    /**
     * fetch() paces itself against real time (sleeps until each tick is "due" - see
     * GeneratorSplitReader). Without this, fetchMany() in a loop would mean every test here
     * actually sleeps in real time - fine for one or two fetch() calls, but fetch_many-style
     * tests calling it 20-30 times would take tens of real seconds and risk tripping
     * @Timeout. Advancing this manually between calls simulates time passing instantly instead.
     */
    private static final class FakeClock implements LongSupplier {
        private long time;

        FakeClock(long initial) {
            this.time = initial;
        }

        @Override
        public long getAsLong() {
            return time;
        }

        void advance(long millis) {
            time += millis;
        }
    }

    private record ReaderAndClock(GeneratorSplitReader reader, FakeClock clock) {}

    private static List<GeneratedEvent> fetchOnce(GeneratorSplitReader reader) throws Exception {
        RecordsWithSplitIds<GeneratedEvent> records = reader.fetch();
        List<GeneratedEvent> events = new ArrayList<>();
        String splitId;
        while ((splitId = records.nextSplit()) != null) {
            GeneratedEvent event;
            while ((event = records.nextRecordFromSplit()) != null) {
                events.add(event);
            }
        }
        return events;
    }

    private static List<GeneratedEvent> fetchMany(ReaderAndClock rc, int times) throws Exception {
        List<GeneratedEvent> all = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            all.addAll(fetchOnce(rc.reader()));
            rc.clock().advance(INTERVAL_MS);
        }
        return all;
    }

    private static ReaderAndClock newReaderWithSplit(
            long maxDisorderMs, LatenessConfig lateness, IdlePartitionConfig idlePartition) throws Exception {
        FakeClock clock = new FakeClock(0L);
        GeneratorSplitReader reader =
                new GeneratorSplitReader(INTERVAL_MS, maxDisorderMs, lateness, idlePartition, clock);
        reader.handleSplitsChanges(
                new SplitsAddition<>(List.of(new GeneratorSplit(SPLIT_ID, 0L))));
        return new ReaderAndClock(reader, clock);
    }

    /**
     * eventTime = baseTime + sequence * intervalMs - lateOffsetMs + disorderOffsetMs, and
     * baseTime is constant for every event a single reader instance ever produces. Rearranged,
     * this quantity is the same for every event regardless of sequence/offsets - a way to check
     * the formula holds without needing to know baseTime's actual value (it's a private field
     * seeded from the injected clock, not something a test should reach into via reflection).
     */
    private static long impliedBaseTime(GeneratedEvent event) {
        return event.eventTime() - event.sequence() * INTERVAL_MS + event.lateOffsetMs() - event.disorderOffsetMs();
    }

    @Test
    void fetch_plainInOrder_producesSequentialEventsWithNoOffsets() throws Exception {
        ReaderAndClock rc = newReaderWithSplit(0, null, null);

        List<GeneratedEvent> events = fetchMany(rc, 5);

        assertEquals(5, events.size());
        for (int i = 0; i < events.size(); i++) {
            GeneratedEvent event = events.get(i);
            assertEquals(i, event.sequence());
            assertEquals(0L, event.lateOffsetMs());
            assertEquals(0L, event.disorderOffsetMs());
        }
        long baseTime = impliedBaseTime(events.get(0));
        for (GeneratedEvent event : events) {
            assertEquals(baseTime, impliedBaseTime(event));
        }
    }

    @Test
    void fetch_idlePartitionMatchingSplit_skipsConfiguredSequenceRange() throws Exception {
        IdlePartitionConfig idlePartition = new IdlePartitionConfig(SPLIT_ID, 3, 2);
        ReaderAndClock rc = newReaderWithSplit(0, null, idlePartition);

        List<GeneratedEvent> events = fetchMany(rc, 6);

        List<Long> sequences = events.stream().map(GeneratedEvent::sequence).toList();
        assertEquals(List.of(0L, 1L, 2L, 5L), sequences);
    }

    @Test
    void fetch_idlePartitionNonMatchingSplit_doesNotSkipAnything() throws Exception {
        IdlePartitionConfig idlePartition = new IdlePartitionConfig("some-other-partition", 3, 2);
        ReaderAndClock rc = newReaderWithSplit(0, null, idlePartition);

        List<GeneratedEvent> events = fetchMany(rc, 6);

        List<Long> sequences = events.stream().map(GeneratedEvent::sequence).toList();
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L), sequences);
    }

    @Test
    void fetch_idlePartitionNull_doesNotSkipOrThrow() throws Exception {
        ReaderAndClock rc = newReaderWithSplit(0, null, null);

        List<GeneratedEvent> events = fetchMany(rc, 4);

        List<Long> sequences = events.stream().map(GeneratedEvent::sequence).toList();
        assertEquals(List.of(0L, 1L, 2L, 3L), sequences);
    }

    @Test
    void fetch_latenessAlwaysLateFixedOffset_appliesExactOffset() throws Exception {
        LatenessConfig lateness = new LatenessConfig(1.0, 5000, 5000);
        ReaderAndClock rc = newReaderWithSplit(0, lateness, null);

        List<GeneratedEvent> events = fetchMany(rc, 5);

        assertEquals(5, events.size());
        for (GeneratedEvent event : events) {
            assertEquals(5000L, event.lateOffsetMs());
        }
        long baseTime = impliedBaseTime(events.get(0));
        for (GeneratedEvent event : events) {
            assertEquals(baseTime, impliedBaseTime(event));
        }
    }

    @Test
    void fetch_latenessWithDistinctMinMax_respectsBothBoundsAndVaries() throws Exception {
        LatenessConfig lateness = new LatenessConfig(1.0, 3000, 7000);
        ReaderAndClock rc = newReaderWithSplit(0, lateness, null);

        List<GeneratedEvent> events = fetchMany(rc, 30);

        assertEquals(30, events.size());
        for (GeneratedEvent event : events) {
            assertTrue(event.lateOffsetMs() >= 3000 && event.lateOffsetMs() <= 7000,
                    "lateOffsetMs out of [min, max] bounds: " + event.lateOffsetMs());
        }
        long distinctOffsets = events.stream().map(GeneratedEvent::lateOffsetMs).distinct().count();
        assertTrue(distinctOffsets > 1,
                "expected varied offsets across samples, got only one distinct value - "
                        + "min/max may have collapsed to a single bound");
    }

    @Test
    void fetch_latenessNull_neverAppliesLateOffsetAndDoesNotThrow() throws Exception {
        ReaderAndClock rc = newReaderWithSplit(0, null, null);

        List<GeneratedEvent> events = fetchMany(rc, 5);

        for (GeneratedEvent event : events) {
            assertEquals(0L, event.lateOffsetMs());
        }
    }

    @Test
    @Timeout(5)
    void fetch_latenessWithUnboundedMaxOffset_rejectionSamplingRespectsMinimumAndTerminates() throws Exception {
        LatenessConfig lateness = new LatenessConfig(1.0, 100, Long.MAX_VALUE);
        ReaderAndClock rc = newReaderWithSplit(0, lateness, null);

        List<GeneratedEvent> events = fetchMany(rc, 20);

        assertEquals(20, events.size());
        for (GeneratedEvent event : events) {
            assertTrue(event.lateOffsetMs() >= 100,
                    "lateOffsetMs should be >= minLateOffsetMs, was " + event.lateOffsetMs());
        }
    }

    @Test
    void fetch_disorderWithinBounds_appliesSymmetricJitterAndEventTimeFormulaHolds() throws Exception {
        long maxDisorderMs = 1000;
        ReaderAndClock rc = newReaderWithSplit(maxDisorderMs, null, null);

        List<GeneratedEvent> events = fetchMany(rc, 20);

        assertEquals(20, events.size());
        for (GeneratedEvent event : events) {
            assertTrue(event.disorderOffsetMs() >= -maxDisorderMs && event.disorderOffsetMs() <= maxDisorderMs,
                    "disorderOffsetMs out of bounds: " + event.disorderOffsetMs());
        }
        long baseTime = impliedBaseTime(events.get(0));
        for (GeneratedEvent event : events) {
            assertEquals(baseTime, impliedBaseTime(event));
        }
    }

    @Test
    void fetch_advancesSplitSequenceByOneEvenWhenIdle() throws Exception {
        IdlePartitionConfig idlePartition = new IdlePartitionConfig(SPLIT_ID, 1, 2);
        FakeClock clock = new FakeClock(0L);
        GeneratorSplitReader reader =
                new GeneratorSplitReader(INTERVAL_MS, 0, null, idlePartition, clock);
        GeneratorSplit split = new GeneratorSplit(SPLIT_ID, 0L);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split)));

        fetchMany(new ReaderAndClock(reader, clock), 4);

        // sequences 1 and 2 are skipped (idle), but nextSequence still advances by one per
        // fetch() call either way - it tracks "how many fetch() calls have happened for this
        // split", not "how many events were actually emitted."
        assertEquals(4L, split.nextSequence());
    }
}
