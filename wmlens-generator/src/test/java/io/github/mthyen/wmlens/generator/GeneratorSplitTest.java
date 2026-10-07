package io.github.mthyen.wmlens.generator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneratorSplitTest {

    @Test
    void constructorAndGetters_exposeSplitIdAndNextSequence() {
        GeneratorSplit split = new GeneratorSplit("partition-0", 5L);

        assertEquals("partition-0", split.splitId());
        assertEquals(5L, split.nextSequence());
    }

    @Test
    void setNextSequence_advancesToAHigherValue() {
        GeneratorSplit split = new GeneratorSplit("partition-0", 0L);

        split.setNextSequence(7L);

        assertEquals(7L, split.nextSequence());
    }

    @Test
    void setNextSequence_allowsSettingToTheSameValue() {
        GeneratorSplit split = new GeneratorSplit("partition-0", 3L);

        split.setNextSequence(3L);

        assertEquals(3L, split.nextSequence());
    }

    @Test
    void setNextSequence_rejectsMovingBackwards() {
        GeneratorSplit split = new GeneratorSplit("partition-0", 10L);

        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> split.setNextSequence(9L));

        assertTrue(ex.getMessage().contains("must not move backwards"),
                "expected message to explain the invariant, was: " + ex.getMessage());
        // the failed call must not have mutated state
        assertEquals(10L, split.nextSequence());
    }

    @Test
    void toString_includesSplitIdAndNextSequence() {
        GeneratorSplit split = new GeneratorSplit("partition-2", 42L);

        String s = split.toString();

        assertTrue(s.contains("partition-2"), "toString() should mention the split id: " + s);
        assertTrue(s.contains("42"), "toString() should mention nextSequence: " + s);
    }
}
