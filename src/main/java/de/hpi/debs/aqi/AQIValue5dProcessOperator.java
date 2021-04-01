package de.hpi.debs.aqi;

import de.hpi.debs.Event;
import de.hpi.debs.slicing.AqiWindowState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class AQIValue5dProcessOperator extends KeyedProcessOperator<String, AQIValue24h, AQIValue5d> {

    protected ValueState<AqiWindowState> state;

    public final long start;
    public long size;
    public long step;
    public long doubleStep;
    public int vDeltaIdx;

    public AQIValue5dProcessOperator(long start) {
        super(
            new KeyedProcessFunction<>() {
                @Override
                public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) {
                    // do nothing as we are doing everything in the operator
                }
            }
        );

        this.start = start;
        this.size = 432000000;
        this.step = 300000;
        this.doubleStep = 2 * step;
        this.vDeltaIdx = (int)(size / step) - 1;
    }

    public AQIValue5dProcessOperator(long start, long size, long step) {
        super(
                new KeyedProcessFunction<>() {
                    @Override
                    public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) {
                        // do nothing as we are doing everything in the operator
                    }
                }
        );

        this.start = start;
        this.size = size;
        this.step = step;
        this.doubleStep = 2 * this.step;
        this.vDeltaIdx = (int)(this.size / this.step) - 1;
    }

    @Override
    public void open() {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", AqiWindowState.class));
    }

    @Override
    public void processElement(StreamRecord<AQIValue24h> value) throws Exception {
        if (state.value() != null && value.getTimestamp() < state.value().getLastWatermark()) // ignore late events
            return;

        if (value.getValue().isWatermark()) { // emit results on watermark arrival
            long wm = value.getTimestamp();
            long lw = state.value().getLastWatermark();
            state.value().updateLastWatermark(wm);
            AqiWindowState window = state.value();

            double deltaWindowSum;
            int deltaWindowCount;
            int startIdx;
            long curWindowEnd;
            boolean active;

            // go to first slice that need to emit window result
            int i = window.getSlicesNr() - 1;

            while (0 < i && lw < window.getEndOfSlice(i - 1)) // Do we need to emit previous window?
                --i;

            curWindowEnd = window.getEndOfSlice(i);

            // emit results of pending windows
            while (curWindowEnd < wm) {
                active = false;

                if (0 < i) { // use pre-aggregate if possible
                    if (window.preAggregate(i)) {
                        // get aggregate of previous windows
                        deltaWindowSum = window.getAqiSlice(i - 1).getWindowSum();
                        deltaWindowCount = window.getAqiSlice(i - 1).getWindowCount();

                        startIdx = i - 1 - vDeltaIdx; // first slice of previous window

                        if (0 <= startIdx) { // check if there is a slice that we need to subtract
                            deltaWindowSum -= window.getAqiSlice(startIdx).getSum();
                            deltaWindowCount -= window.getAqiSlice(startIdx).getCount();
                        }

                        window.addPreAggregate(i, deltaWindowSum, deltaWindowCount);
                    }

                    if (!window.getAqiSlice(i - 1).isEmpty())
                        active = true;
                }

                if (!window.getAqiSlice(i).isEmpty()) // check if window is active
                    active = true;

                if (active && lw < curWindowEnd) {
                    double avgAqi = window.getAqiSlice(i).getWindowAvg();
                    double curAqi = window.getAqiSlice(i).getSum();

                    output.collect(new StreamRecord<>(new AQIValue5d(
                            avgAqi,
                            curAqi,
                            curWindowEnd,
                            false,
                            (String) getCurrentKey()),
                            curWindowEnd
                    ));
                }

                ++i;

                if (i == window.getSlicesNr()) // check if there where events received in the past v5minInSec
                    state.value().addSlice(step);

                curWindowEnd = state.value().getEndOfSlice(i);
            }

            // emit "watermark window"
            deltaWindowSum = 0.0;
            deltaWindowCount = 0;
            active = false;

            if (0 < i) { // use pre-aggregated results of previous windows if not already done
                deltaWindowSum = window.getAqiSlice(i - 1).getWindowSum();
                deltaWindowCount = window.getAqiSlice(i - 1).getWindowCount();

                startIdx = i - 1 - vDeltaIdx; // first slice of previous window

                if (0 <= startIdx) { // check if there is a slice that we need to subtract from first 24h window
                    deltaWindowSum -= window.getAqiSlice(startIdx).getSum();
                    deltaWindowCount -= window.getAqiSlice(startIdx).getCount();

                    long watermarkWindowStart = wm - size;

                    for (Event event : window.getAqiSlice(startIdx).getEvents()) {
                        if (event.getTimestamp() < watermarkWindowStart) {
                            deltaWindowSum -= event.getValue();
                            --deltaWindowCount;
                        }
                    }
                }

                // check if "watermark window" is active
                if (!window.getAqiSlice(i - 1).isEmpty())
                    active = true;

                if (1 < i && !window.getAqiSlice(i - 2).isEmpty()) {
                    long before10MinInSec = wm - doubleStep;

                    for (Event event : window.getAqiSlice(i - 2).getEvents())
                        if (before10MinInSec <= event.getTimestamp()) {
                            active = true;
                            break;
                        }
                }
            }

            // add events from last slice that belong to "watermark window"
            for (Event event : window.getAqiSlice(i).getEvents()) {
                if (event.getTimestamp() < wm) {
                    deltaWindowSum += event.getValue();
                    ++deltaWindowCount;
                    active = true;
                }
            }

            if (active) { // check if in last v24hInSec where tuples emitted
                double avgAqi = deltaWindowSum / deltaWindowCount;
                double curAqi = window.getAqiSlice(i).getSum();

                output.collect(new StreamRecord<>(new AQIValue5d(
                        avgAqi,
                        curAqi,
                        wm,
                        true,
                        (String) getCurrentKey()),
                        wm
                ));
            }

            // remove slices that are already emitted and disjoint with all remaining windows that will be emitted
            state.value().removeSlices(wm - size);

            // additionally remove all empty slices from tail
            state.value().removeEmptyTail();

            // clear state for this key if there are no slices anymore
            if (state.value().getSlicesNr() == 0) {
                state.clear();
            }

            return;
        }

        if (state.value() == null) {
            long newStart = (value.getTimestamp() - start) % step;
            newStart = value.getTimestamp() - newStart; // get correct start of window in case city measures very late
            state.update(new AqiWindowState(
                    (String) getCurrentKey(),
                    newStart,
                    newStart + step
            ));
        }

        // add tuple that actually has a matching city
        int i = state.value().getSlicesNr() - 1;
        int in = state.value().in(i, value.getTimestamp());

        // search for correct slice
        while (in != 0) {
            if (in < 0) { // go one slice to the past
                --i;
            } else {
                state.value().addSlice(step); // add new slice to end

                ++i;
            }

            in = state.value().in(i, value.getTimestamp());
        }

        // update slice by new event
        state.value().addMeasure(i, value.getValue().getAQI(), value.getTimestamp());
    }
}