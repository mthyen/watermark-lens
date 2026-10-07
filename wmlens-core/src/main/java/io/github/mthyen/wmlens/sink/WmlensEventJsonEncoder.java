package io.github.mthyen.wmlens.sink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.api.common.serialization.Encoder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes one {@link WmlensEvent} per line as JSON (JSONL), for use with
 * {@link io.github.mthyen.wmlens.api.WatermarkLens#jsonlSink}.
 */
public class WmlensEventJsonEncoder implements Encoder<WmlensEvent> {
    private transient ObjectMapper mapper;

    @Override
    public void encode(WmlensEvent element, OutputStream stream) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
            // writeValue(OutputStream, Object) closes the stream after writing by default
            // (JsonGenerator.Feature.AUTO_CLOSE_TARGET) - fatal here, since the same stream is
            // reused across many events.
            mapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        }
        mapper.writeValue(stream, element);
        stream.write('\n');
    }
}