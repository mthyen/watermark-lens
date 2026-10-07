package io.github.mthyen.wmlens.generator;

/**
 * Controls how often, and how far behind, {@link GeneratorSplitReader} generates late events.
 *
 * @param lateFraction probability (0.0-1.0) that any given generated event is late
 * @param minLateOffsetMs minimum lateness applied to a late event, in milliseconds
 * @param maxLateOffsetMs maximum lateness applied to a late event, in milliseconds - pass
 *     {@code Long.MAX_VALUE} for an unbounded upper end
 */
public record LatenessConfig(double lateFraction, long minLateOffsetMs, long maxLateOffsetMs)
    implements java.io.Serializable {}
