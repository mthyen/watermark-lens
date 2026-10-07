# Watermark Lens

Watermark Lens is a diagnostic library for Apache Flink jobs. Drop it into an existing
streaming pipeline and it records how your job's watermarks actually advance, when and why
your windows fire, and which elements get dropped for arriving too late - without changing
what the pipeline computes or outputs.

Event time, watermarks, and lateness are some of the hardest things to build intuition for in
Flink, because the behavior is invisible unless you go looking for it. Watermark Lens captures
what the watermark/window/late-data machinery is actually doing as structured events, then
renders that capture as an interactive timeline.

## Requirements

- Java 17
- Apache Flink 2.3.0

## Modules

| Module | What it is |
|---|---|
| `wmlens-core` | The library itself: `WatermarkLens`, the event model, and the JSONL sink. This is the one dependency you'd actually ship. |
| `wmlens-generator` | A configurable synthetic event source (disorder, a controllable late fraction, idle partitions) for exercising and demonstrating the library without a real data source. |
| `wmlens-examples` | One runnable Flink job per concept - generator output, the watermark tap, window/lateness instrumentation, JSONL capture, parallelism, idle partitions. |
| `wmlens-viewer` | A single self-contained `index.html` (no build step, no server, nothing uploaded anywhere) that loads a captured JSONL file and renders it as an interactive timeline. |

## Building from source

Not published anywhere yet - clone this repo and build it locally:

```sh
mvn install
```

This also installs `wmlens-core` into your local Maven repository (`~/.m2`), which is what makes
it usable as a dependency from a separate project - see below.

To run one of this repo's own examples, use `exec:exec` with the example's fully-qualified class
name (forks a real `java` process, since Flink's embedded MiniCluster doesn't tolerate running in
Maven's own JVM):

```sh
mvn -pl wmlens-examples exec:exec -DmainClass=io.github.mthyen.wmlens.examples.FullPipelineJsonlExample
```

## Adding it to your own project

After `mvn install` above, add `wmlens-core` as a dependency in your own project's `pom.xml`:

```xml
<dependency>
  <groupId>io.github.mthyen</groupId>
  <artifactId>wmlens-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`wmlens-core` doesn't bundle Flink itself - it expects your project to already depend on
`flink-streaming-java`, `flink-runtime`, and `flink-core`, the same way any Flink job does.

## Usage

**No pipeline of your own to try this on yet?** Run `WatermarkTapPrintExample` - it's the
simplest of the bundled examples, a generated stream with nothing but `tap()` attached, printing
both the stream and the watermark events it observes:

```sh
mvn -pl wmlens-examples exec:exec -DmainClass=io.github.mthyen.wmlens.examples.WatermarkTapPrintExample
```

**Otherwise, start with your own stream.** `tap()` doesn't change what it emits - `tapped` below
produces exactly what `source` would have, so the rest of your pipeline doesn't need to change:

```java
import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

DataStream<MyEvent> source = ...; // however you already build this

SingleOutputStreamOperator<MyEvent> tapped =
        WatermarkLens.tap(source, new WatermarkLensConfig("my-tap"));

tapped.getSideOutput(WmlensEvent.EVENTS_TAG)
        .sinkTo(WatermarkLens.jsonlSink("/path/to/output"));

env.enableCheckpointing(2000); // required for jsonlSink() to ever roll/finalize files

// Use `tapped` anywhere you'd have used `source` from here on - keyBy, window, sink,
// whatever the rest of your pipeline already does.
```

**Want window firings and late-dropped elements too, not just watermark advances?** `tap()`,
`instrument()`, and `observeLateData()` are independent - combine all three and union their side
outputs into one capture:

```java
import io.github.mthyen.wmlens.api.WatermarkLens;
import io.github.mthyen.wmlens.api.WatermarkLensConfig;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(2000); // required for jsonlSink() to ever roll/finalize files

DataStream<MyEvent> source = ...;

// Observe watermark advances and per-subtask element throughput.
SingleOutputStreamOperator<MyEvent> tapped =
        WatermarkLens.tap(source, new WatermarkLensConfig("my-tap"));

// Observe window firings (on-time vs. late-update) on an existing windowed pipeline.
SingleOutputStreamOperator<MyResult> windowed = tapped.keyBy(...)
        .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
        .allowedLateness(Duration.ofSeconds(3))
        .sideOutputLateData(lateTag)
        .process(WatermarkLens.instrument(new MyWindowFunction(), new WatermarkLensConfig("my-window")));

// Observe elements dropped for arriving after allowedLateness. Must be re-keyed with the
// *same* KeySelector as the windowing above - see WatermarkLens.observeLateData's Javadoc.
SingleOutputStreamOperator<MyEvent> lateTapped = WatermarkLens.observeLateData(
        windowed.getSideOutput(lateTag).keyBy(...), new WatermarkLensConfig("my-late-tap"));

// Union every tap's diagnostic side output into one capture file.
DataStream<WmlensEvent> allEvents = tapped.getSideOutput(WmlensEvent.EVENTS_TAG)
        .union(windowed.getSideOutput(WmlensEvent.EVENTS_TAG))
        .union(lateTapped.getSideOutput(WmlensEvent.EVENTS_TAG));

allEvents.sinkTo(WatermarkLens.jsonlSink("/path/to/output"));
```

See `wmlens-examples` for complete, runnable versions of this and other scenarios (plain
generator output, idle partitions, parallelism).

Then open `wmlens-viewer/index.html` in a browser and drop in the resulting JSONL file(s) to see
a timeline chart (with zoom) and a filterable event table.

### Sample data

`wmlens-viewer/sample-data` has captured JSONL files if you want to try the viewer before running
a job yourself - open `index.html` and drop one in:

| File | What it shows |
|---|---|
| `sample.jsonl` | A single `tap()`, nothing else - the basic watermark/throughput timeline. |
| `sample-full.jsonl` | `tap()` + `instrument()` + `observeLateData()` together - all four event types, three operators, one subtask each. |
| `sample-large.jsonl` | ~7,000 events from the same full pipeline, run longer - exercises the chart's zoom and the table's "Show more" paging. |
| `sample-parallel.jsonl` | Three parallel subtasks tapped directly off the source, no shuffle - three independent, non-interleaved watermark lines. |
| `sample-idle-no-withidleness.jsonl` | Two subtasks merged after a `keyBy`, one of them idle for a stretch - the merged watermark visibly stalls, then jumps once the idle partition catches up. |
| `sample-idle-with-withidleness.jsonl` | The same idle scenario with `WatermarkStrategy.withIdleness()` added - the stall disappears, since the idle subtask stops counting against the merged watermark. |

The last two are worth loading side by side - same idle-partition setup, and `withIdleness()` is
the only difference between them.

## Project status

All five original milestones (generator, watermark tap, window/lateness instrumentation, the
JSONL sink + viewer, and parallelism/idle-partition experiments) are done. See `FUTURE_WORK.md`
for ideas deliberately deferred rather than forgotten.

## License

Apache License 2.0 - see `LICENSE`.
