package de.hpi.debs.aqi;

import de.hpi.debs.Event;
import de.hpi.debs.MeasurementOwn;
import de.hpi.debs.slicing.ParticleWindowState;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class AQIValue24hProcessOperator extends KeyedProcessOperator<String, MeasurementOwn, AQIValue24h> {

    protected ValueState<ParticleWindowState> state;

    public final long start;
    public long size;
    public long step;
    public long doubleStep;
    public int vDeltaIdx;

    public AQIValue24hProcessOperator(long start) {
        super(
            new KeyedProcessFunction<>() {
                @Override
                public void processElement(MeasurementOwn value, Context ctx, Collector<AQIValue24h> out) {
                    // do nothing as we are doing everything in the operator
                }
            }
        );

        this.start = start;
        this.size = 86400000;
        this.step = 300000;
        this.doubleStep = 2 * step;
        this.vDeltaIdx = (int)(size / step) - 1;
    }

    public AQIValue24hProcessOperator(long start, long size, long step) {
        super(
                new KeyedProcessFunction<>() {
                    @Override
                    public void processElement(MeasurementOwn value, Context ctx, Collector<AQIValue24h> out) {
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
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", ParticleWindowState.class));
    }

    @Override
    public void processElement(StreamRecord<MeasurementOwn> value) throws Exception {
        if (state.value() != null && value.getTimestamp() < state.value().getLastWatermark()) // ignore late events
            return;

        if (value.getValue().isWatermark()) { // emit results on watermark arrival
            if (state.value() == null) // in case no records are processed beforehand
                return;

            long wm = value.getTimestamp();
            long lw = state.value().getLastWatermark();
            state.value().updateLastWatermark(wm);
            ParticleWindowState window = state.value();

            double deltaWindowSum1;
            double deltaWindowSum2;
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
                    if (state.value().preAggregate(i)) {
                        // get aggregate of previous windows
                        deltaWindowSum1 = window.getP1Slice(i - 1).getWindowSum();
                        deltaWindowSum2 = window.getP2Slice(i - 1).getWindowSum();
                        deltaWindowCount = window.getP2Slice(i - 1).getWindowCount();

                        startIdx = i - 1 - vDeltaIdx; // first slice of previous window

                        if (0 <= startIdx) { // check if there is a slice that we need to subtract
                            deltaWindowSum1 -= window.getP1Slice(startIdx).getSum();
                            deltaWindowSum2 -= window.getP2Slice(startIdx).getSum();
                            deltaWindowCount -= window.getP2Slice(startIdx).getCount();
                        }

                        state.value().addPreAggregate(i, deltaWindowSum1, deltaWindowSum2, deltaWindowCount);
                    }

                    if (!window.getP1Slice(i - 1).isEmpty())
                        active = true;
                }

                if (!window.getP1Slice(i).isEmpty()) // check if window is active
                    active = true;

                if (active && lw < curWindowEnd) {
                    float avgP1 = (float) state.value().getP1Slice(i).getWindowAvg();
                    float avgP2 = (float) state.value().getP2Slice(i).getWindowAvg();

                    output.collect(new StreamRecord<>(new AQIValue24h(
                            AQICalculator.getAQI(avgP2, avgP1),
                            AQICalculator.getAQI10(avgP1),
                            AQICalculator.getAQI25(avgP2),
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
            deltaWindowSum1 = 0.0;
            deltaWindowSum2 = 0.0;
            deltaWindowCount = 0;
            active = false;

            if (0 < i) { // use pre-aggregated results of previous windows if not already done
                deltaWindowSum1 = window.getP1Slice(i - 1).getWindowSum();
                deltaWindowSum2 = window.getP2Slice(i - 1).getWindowSum();
                deltaWindowCount = window.getP2Slice(i - 1).getWindowCount();

                startIdx = i - 1 - vDeltaIdx; // first slice of previous window

                if (0 <= startIdx) { // check if there is a slice that we need to subtract from first 24h window
                    deltaWindowSum1 -= window.getP1Slice(startIdx).getSum();
                    deltaWindowSum2 -= window.getP2Slice(startIdx).getSum();
                    deltaWindowCount -= window.getP2Slice(startIdx).getCount();

                    long watermarkWindowStart = wm - size;

                    for (Event event : window.getP1Slice(startIdx).getEvents()) {
                        if (event.getTimestamp() < watermarkWindowStart) {
                            deltaWindowSum1 -= event.getValue();
                            --deltaWindowCount;
                        }
                    }

                    for (Event event : window.getP2Slice(startIdx).getEvents()) {
                        if (event.getTimestamp() < watermarkWindowStart) {
                            deltaWindowSum2 -= event.getValue();
                        }
                    }
                }

                // check if "watermark window" is active
                if (!window.getP1Slice(i - 1).isEmpty())
                    active = true;

                if (1 < i && !window.getP1Slice(i - 2).isEmpty()) {
                    long before10MinInSec = wm - doubleStep;

                    for (Event event : window.getP1Slice(i - 2).getEvents())
                        if (before10MinInSec <= event.getTimestamp()) {
                            active = true;
                            break;
                        }
                }
            }

            // add events from last slice that belong to "watermark window"
            for (Event event : window.getP1Slice(i).getEvents()) {
                if (event.getTimestamp() < wm) {
                    deltaWindowSum1 += event.getValue();
                    ++deltaWindowCount;
                    active = true;
                }
            }

            // again same for larger particles
            for (Event event : window.getP2Slice(i).getEvents())
                if (event.getTimestamp() < wm)
                    deltaWindowSum2 += event.getValue();

            if (active) { // check if in last v24hInSec where tuples emitted
                float avgP1 = (float) deltaWindowSum1 / deltaWindowCount;
                float avgP2 = (float) deltaWindowSum2 / deltaWindowCount;

                output.collect(new StreamRecord<>(new AQIValue24h(
                        AQICalculator.getAQI(avgP2, avgP1),
                        AQICalculator.getAQI10(avgP1),
                        AQICalculator.getAQI25(avgP2),
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
            state.update(new ParticleWindowState(
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
        state.value().addMeasure(i, value.getValue().getP1(), value.getValue().getP2(), value.getTimestamp());
    }
}
