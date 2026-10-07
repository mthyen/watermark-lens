package io.github.mthyen.wmlens.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mthyen.wmlens.event.EventBucketEvent;
import io.github.mthyen.wmlens.event.EventEnvelope;
import io.github.mthyen.wmlens.event.LateDropEvent;
import io.github.mthyen.wmlens.event.WatermarkEvent;
import io.github.mthyen.wmlens.event.WindowFiredEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import io.github.mthyen.wmlens.event.FiringType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WmlensEventJsonEncoderTest {

    /**
     * ObjectMapper.writeValue(OutputStream, Object) closes the stream after writing by default
     * (JsonGenerator.Feature.AUTO_CLOSE_TARGET) - the encoder disables that so the same stream
     * can be reused across many events in one file. This wrapper makes that behavior directly
     * assertable instead of only discoverable by a FileSink crash-looping in production, which
     * is how it was actually found - see FileSink's rolling behavior and this class's own
     * comment for the full story.
     */
    private static final class CloseTrackingOutputStream extends ByteArrayOutputStream {
        private boolean closed = false;

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }

        boolean wasClosed() {
            return closed;
        }
    }

    private static final EventEnvelope ENVELOPE =
            new EventEnvelope(EventEnvelope.CURRENT_VERSION, "run-1", 1_000L, "test-op", 0);

    private final ObjectMapper mapper = new ObjectMapper();
    private final WmlensEventJsonEncoder encoder = new WmlensEventJsonEncoder();

    private JsonNode encodeAndParse(WmlensEvent event) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        encoder.encode(event, out);
        return mapper.readTree(out.toByteArray());
    }

    @Test
    void encode_watermarkEvent_includesTypeDiscriminatorAndFields() throws IOException {
        JsonNode node = encodeAndParse(new WatermarkEvent(ENVELOPE, 5_000L));

        assertEquals("watermark", node.get("type").asText());
        assertEquals(5_000L, node.get("watermark").asLong());
        assertEquals("test-op", node.get("envelope").get("operator").asText());
    }

    @Test
    void encode_eventBucketEvent_includesTypeDiscriminatorAndFields() throws IOException {
        JsonNode node = encodeAndParse(new EventBucketEvent(ENVELOPE, 3L, 100L, 300L, 1L));

        assertEquals("event_bucket", node.get("type").asText());
        assertEquals(3L, node.get("count").asLong());
        assertEquals(100L, node.get("minEventTime").asLong());
        assertEquals(300L, node.get("maxEventTime").asLong());
        assertEquals(1L, node.get("behindWatermark").asLong());
    }

    @Test
    void encode_windowFiredEvent_includesTypeDiscriminatorAndFields() throws IOException {
        JsonNode node = encodeAndParse(
                new WindowFiredEvent(ENVELOPE, "k", 0L, 5000L, 7L, 4800L, FiringType.LATE_UPDATE));

        assertEquals("window_fired", node.get("type").asText());
        assertEquals("k", node.get("key").asText());
        assertEquals("LATE_UPDATE", node.get("firing").asText());
    }

    @Test
    void encode_lateDropEvent_includesTypeDiscriminatorAndFields() throws IOException {
        JsonNode node = encodeAndParse(new LateDropEvent(ENVELOPE, "k", 100L, 900L, 800L));

        assertEquals("late_drop", node.get("type").asText());
        assertEquals(800L, node.get("latenessMs").asLong());
    }

    @Test
    void encode_appendsNewlineAfterEachEvent() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        encoder.encode(new WatermarkEvent(ENVELOPE, 1L), out);

        assertTrue(out.toString().endsWith("\n"));
    }

    @Test
    void encode_doesNotCloseTheStream_acrossMultipleEvents() throws IOException {
        CloseTrackingOutputStream out = new CloseTrackingOutputStream();

        encoder.encode(new WatermarkEvent(ENVELOPE, 1L), out);
        encoder.encode(new WatermarkEvent(ENVELOPE, 2L), out);
        encoder.encode(new WatermarkEvent(ENVELOPE, 3L), out);

        assertFalse(out.wasClosed(),
                "encode() must never close the shared sink stream - see AUTO_CLOSE_TARGET in the class under test");

        String[] lines = out.toString().split("\n");
        assertEquals(3, lines.length);
        for (String line : lines) {
            JsonNode node = mapper.readTree(line);
            assertEquals("watermark", node.get("type").asText());
        }
        List<Long> watermarks = List.of(
                mapper.readTree(lines[0]).get("watermark").asLong(),
                mapper.readTree(lines[1]).get("watermark").asLong(),
                mapper.readTree(lines[2]).get("watermark").asLong());
        assertEquals(List.of(1L, 2L, 3L), watermarks);
    }
}
