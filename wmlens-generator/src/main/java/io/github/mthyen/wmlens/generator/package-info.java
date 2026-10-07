/**
 * A controllable Flink source for testing: configurable event-time disorder, late-event
 * fraction, and idle partitions, emitted alongside the ground truth needed to check watermark
 * and window behavior against what should have happened.
 */
package io.github.mthyen.wmlens.generator;
