/**
 * Instrumented {@code ProcessWindowFunction} wrapper that records window firings (on-time and
 * late-triggered updates) and reports late data pulled from a {@code sideOutputLateData} tag,
 * without changing the wrapped function's output.
 */
package io.github.mthyen.wmlens.window;
