package io.github.mthyen.wmlens.window;

import io.github.mthyen.wmlens.event.FiringType;
import io.github.mthyen.wmlens.event.WindowFiredEvent;
import io.github.mthyen.wmlens.event.WmlensEvent;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class InstrumentedProcessWindowFunctionTest {

    /** A trivial real wrapped function - counts elements, like the examples do. */
    private static class CountingWindowFunction extends ProcessWindowFunction<Long, Long, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<Long> out) {
            long count = 0;
            for (Long ignored : elements) count++;
            out.collect(count);
        }
    }

    private ProcessWindowFunction<Long, Long, String, TimeWindow>.Context context;
    private ValueState<Boolean> firedState;
    private RuntimeContext runtimeContext;
    private Collector<Long> collector;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        context = (ProcessWindowFunction<Long, Long, String, TimeWindow>.Context)
                mock(ProcessWindowFunction.Context.class);
        firedState = (ValueState<Boolean>) mock(ValueState.class);
        KeyedStateStore windowState = mock(KeyedStateStore.class);
        when(windowState.getState(any(ValueStateDescriptor.class))).thenReturn(firedState);
        when(context.windowState()).thenReturn(windowState);
        when(context.window()).thenReturn(new TimeWindow(0L, 5000L));
        when(context.currentWatermark()).thenReturn(5000L);

        TaskInfo taskInfo = mock(TaskInfo.class);
        when(taskInfo.getIndexOfThisSubtask()).thenReturn(0);
        runtimeContext = mock(RuntimeContext.class);
        when(runtimeContext.getTaskInfo()).thenReturn(taskInfo);

        collector = mock(Collector.class);
    }

    private InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> instrumented(
            ProcessWindowFunction<Long, Long, String, TimeWindow> wrapped) {
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn =
                new InstrumentedProcessWindowFunction<>(wrapped, "window-op", "run-1");
        fn.setRuntimeContext(runtimeContext);
        return fn;
    }

    @Test
    void process_firstFiringForAWindow_isOnTimeAndUpdatesFiredState() throws Exception {
        when(firedState.value()).thenReturn(null);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(new CountingWindowFunction());

        fn.process("k", context, List.of(10L, 20L, 30L), collector);

        ArgumentCaptor<WmlensEvent> captor = ArgumentCaptor.forClass(WmlensEvent.class);
        verify(context).output(eq(WmlensEvent.EVENTS_TAG), captor.capture());
        WindowFiredEvent event = (WindowFiredEvent) captor.getValue();
        assertEquals(FiringType.ON_TIME, event.firing());
        assertEquals("k", event.key());
        assertEquals(3L, event.count());
        assertEquals(0L, event.windowStart());
        assertEquals(5000L, event.windowEnd());
        assertEquals(5000L, event.watermarkAtFire());
        assertEquals("window-op", event.envelope().operator());
        assertEquals("run-1", event.envelope().runId());
        verify(firedState).update(true);
    }

    @Test
    void process_firingAgainForAWindowAlreadyFired_isLateUpdate() throws Exception {
        when(firedState.value()).thenReturn(true);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(new CountingWindowFunction());

        fn.process("k", context, List.of(10L), collector);

        ArgumentCaptor<WmlensEvent> captor = ArgumentCaptor.forClass(WmlensEvent.class);
        verify(context).output(eq(WmlensEvent.EVENTS_TAG), captor.capture());
        assertEquals(FiringType.LATE_UPDATE, ((WindowFiredEvent) captor.getValue()).firing());
    }

    @Test
    void process_delegatesToTheWrappedFunctionWithTheSameArguments() throws Exception {
        when(firedState.value()).thenReturn(null);
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<Long, Long, String, TimeWindow> wrappedMock = mock(ProcessWindowFunction.class);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(wrappedMock);
        List<Long> elements = List.of(1L, 2L);

        fn.process("k", context, elements, collector);

        verify(wrappedMock).process(eq("k"), eq(context), eq(elements), eq(collector));
    }

    @Test
    void process_realWrappedFunction_stillProducesItsOwnOutput() throws Exception {
        when(firedState.value()).thenReturn(null);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(new CountingWindowFunction());

        fn.process("k", context, List.of(1L, 2L, 3L, 4L), collector);

        verify(collector).collect(4L);
    }

    @Test
    void clear_resetsFiredStateAndDelegatesToTheWrappedFunction() throws Exception {
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<Long, Long, String, TimeWindow> wrappedMock = mock(ProcessWindowFunction.class);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(wrappedMock);

        fn.clear(context);

        verify(firedState).clear();
        verify(wrappedMock).clear(context);
    }

    @Test
    void open_setsTheWrappedFunctionsRuntimeContextAndDelegatesOpen() throws Exception {
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<Long, Long, String, TimeWindow> wrappedMock = mock(ProcessWindowFunction.class);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn =
                new InstrumentedProcessWindowFunction<>(wrappedMock, "window-op", "run-1");
        fn.setRuntimeContext(runtimeContext);
        OpenContext openContext = new OpenContext() {};

        fn.open(openContext);

        verify(wrappedMock).setRuntimeContext(runtimeContext);
        verify(wrappedMock).open(openContext);
    }

    @Test
    void close_delegatesToTheWrappedFunction() throws Exception {
        @SuppressWarnings("unchecked")
        ProcessWindowFunction<Long, Long, String, TimeWindow> wrappedMock = mock(ProcessWindowFunction.class);
        InstrumentedProcessWindowFunction<Long, Long, String, TimeWindow> fn = instrumented(wrappedMock);

        fn.close();

        verify(wrappedMock).close();
    }
}
