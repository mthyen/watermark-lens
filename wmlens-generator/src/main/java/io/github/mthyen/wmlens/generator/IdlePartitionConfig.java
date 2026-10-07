package io.github.mthyen.wmlens.generator;

/**
 * Configures one generator partition to stop producing events for a range of sequence numbers,
 * simulating an idle source partition - see {@link GeneratorSplitReader} for exactly how the skip
 * is applied.
 *
 * @param idlePartitionId the {@link GeneratorSplit#splitId()} to idle - every other partition is
 *     unaffected
 * @param idleAfterSequence the first sequence number to skip
 * @param idleSequenceCount how many consecutive sequence numbers to skip, starting at
 *     {@code idleAfterSequence}
 */
public record IdlePartitionConfig(String idlePartitionId, long idleAfterSequence, long idleSequenceCount)
    implements java.io.Serializable {}
