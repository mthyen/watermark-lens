package io.github.mthyen.wmlens.generator;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorSplitSerializerTest {

    private final GeneratorSplitSerializer serializer = new GeneratorSplitSerializer();

    @Test
    void roundTrip_preservesSplitIdAndNextSequence() throws IOException {
        GeneratorSplit original = new GeneratorSplit("partition-1", 123L);

        byte[] bytes = serializer.serialize(original);
        GeneratorSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(original.splitId(), restored.splitId());
        assertEquals(original.nextSequence(), restored.nextSequence());
    }

    @Test
    void roundTrip_withZeroSequence() throws IOException {
        GeneratorSplit original = new GeneratorSplit("partition-0", 0L);

        GeneratorSplit restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

        assertEquals("partition-0", restored.splitId());
        assertEquals(0L, restored.nextSequence());
    }

    @Test
    void roundTrip_withLargeSequenceDoesNotTruncate() throws IOException {
        GeneratorSplit original = new GeneratorSplit("partition-0", Long.MAX_VALUE - 1);

        GeneratorSplit restored = serializer.deserialize(serializer.getVersion(), serializer.serialize(original));

        assertEquals(Long.MAX_VALUE - 1, restored.nextSequence());
    }

    @Test
    void getVersion_isStableAcrossCalls() {
        assertEquals(serializer.getVersion(), serializer.getVersion());
    }

    @Test
    void deserialize_rejectsAnUnrecognizedVersion() throws IOException {
        byte[] bytes = serializer.serialize(new GeneratorSplit("partition-0", 1L));
        int unknownVersion = serializer.getVersion() + 1;

        IOException ex = assertThrows(IOException.class,
                () -> serializer.deserialize(unknownVersion, bytes));

        assertTrue(ex.getMessage().contains("Unrecognized version"),
                "expected message to explain why, was: " + ex.getMessage());
    }
}
