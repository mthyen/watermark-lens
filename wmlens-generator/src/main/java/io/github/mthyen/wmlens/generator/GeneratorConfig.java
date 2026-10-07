package io.github.mthyen.wmlens.generator;

/**
 * Configuration for {@link GeneratorSource}.
 *
 * @param numPartitions how many independent simulated partitions to generate - one
 *     {@link GeneratorSplit} per partition, handed out one per source subtask
 * @param intervalMs simulated time between consecutive events on each partition, in milliseconds
 * @param ratePerSecond how fast each source subtask emits ready records, in records/second -
 *     independent of {@code intervalMs}; see {@link GeneratorSplitReader} for how the two combine
 * @param maxDisorderMs maximum random jitter applied to each event's timestamp, in either
 *     direction - 0 disables disorder
 * @param lateness controls how often and how far behind generated events arrive late, or
 *     {@code null} to disable lateness
 * @param idlePartition configures one partition to stop producing events for a range of sequence
 *     numbers, or {@code null} to disable idling
 */
public record GeneratorConfig (
    int numPartitions,
    long intervalMs,
    double ratePerSecond,
    long maxDisorderMs,
    LatenessConfig lateness,
    IdlePartitionConfig idlePartition
) implements java.io.Serializable {
}
