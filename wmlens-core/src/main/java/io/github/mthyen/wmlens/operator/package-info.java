/**
 * The watermark tap operator. Extends {@code AbstractStreamOperator} rather than using a
 * {@code ProcessFunction}, because {@code processWatermark} is the only hook that observes
 * every watermark advance; a {@code ProcessFunction} only sees the current watermark when an
 * element happens to arrive, so it can miss advances entirely.
 */
package io.github.mthyen.wmlens.operator;
