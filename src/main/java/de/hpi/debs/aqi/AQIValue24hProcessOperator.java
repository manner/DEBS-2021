package de.hpi.debs.aqi;

import de.hpi.debs.Event;
import de.hpi.debs.MeasurementOwn;
import de.hpi.debs.slicing.ParticleWindowState;
import de.hpi.debs.slicing.Slice;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class AQIValue24hProcessOperator extends KeyedProcessOperator<String, MeasurementOwn, AQIValue24h> {

    private ValueState<ParticleWindowState> state;

    public final long start;
    public static final long v24hInSec = 86400;
    public static final long v5minInSec = 300;
    public static final long v10minInSec = 2 * v5minInSec;
    public static final int vDeltaIdx = (int)(v24hInSec / v5minInSec) - 1;

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
    }

    @Override
    public void open() {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", ParticleWindowState.class));
    }

    @Override
    public void processElement(StreamRecord<MeasurementOwn> value) throws Exception {
        if (state.value() != null && value.getTimestamp() < state.value().getLastWatermark()) // ignore late events
            return;

        if (state.value() == null) {
            long newStart = (value.getTimestamp() - start) % v5minInSec;
            newStart = value.getTimestamp() - newStart; // get correct start of window in case city measures very late
            state.update(new ParticleWindowState(
                    (String) getCurrentKey(),
                    newStart,
                    newStart + v5minInSec
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
                state.value().addSlice(v5minInSec); // add new slice to end

                ++i;
            }

            in = state.value().in(i, value.getTimestamp());
        }

        // update slice by new event
        state.value().addMeasure(i, value.getValue().getP1(), value.getValue().getP2(), value.getTimestamp());
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        if (state == null || state.value() == null)
            return;

        long wm = watermark.getTimestamp();
        long lw = state.value().getLastWatermark();
        state.value().updateLastWatermark(wm);
        ParticleWindowState window = state.value();

        double deltaWindowSum1;
        int deltaWindowCount1;
        double deltaWindowSum2;
        int deltaWindowCount2;
        Slice preWindow1;
        Slice preWindow2;
        Slice preStart1;
        Slice preStart2;
        int startIdx;
        long curWindowEnd;
        boolean active;

        // go to first slice that need to emit window result
        int i = window.getSlicesNr() - 1;

        while (0 < i && lw < window.getEndOfSlice(i))
            --i;

        curWindowEnd = window.getEndOfSlice(i);

        // emit results of windows before the watermark
        while (curWindowEnd < wm) {
            active = false;

            if (0 < i) {
                if (window.getCheckpoint() < i) { // make sure that window is not already using pre-aggregate results
                    preWindow1 = window.getP1Slice(i - 1);
                    preWindow2 = window.getP2Slice(i - 1);

                    deltaWindowSum1 = preWindow1.getWindowSum();
                    deltaWindowCount1 = preWindow1.getWindowCount();
                    deltaWindowSum2 = preWindow2.getWindowSum();
                    deltaWindowCount2 = preWindow2.getWindowCount();

                    startIdx = i - vDeltaIdx - 1;

                    if (0 <= startIdx) { // check if there is a slice that we need to subtract
                        preStart1 = window.getP1Slice(startIdx);
                        preStart2 = window.getP2Slice(startIdx);

                        deltaWindowSum1 -= preStart1.getSum();
                        deltaWindowCount1 -= preStart1.getCount();
                        deltaWindowSum2 -= preStart2.getSum();
                        deltaWindowCount2 -= preStart2.getCount();
                    }

                    window.getP1Slice(i).addToWindow(deltaWindowSum1, deltaWindowCount1);
                    window.getP2Slice(i).addToWindow(deltaWindowSum2, deltaWindowCount2);
                    window.incCheckpoint();
                }

                if (!window.getP1Slice(i - 1).isEmpty())
                    active = true;
            }

            if (!window.getP1Slice(i).isEmpty()) // check if window is active
                active = true;

            float avgP1 = (float) window.getP1Slice(i).getWindowAvg();
            float avgP2 = (float) window.getP2Slice(i).getWindowAvg();

            if (lw < curWindowEnd)
                output.collect(new StreamRecord<>(new AQIValue24h(
                        AQICalculator.getAQI(avgP2, avgP1),
                        curWindowEnd,
                        (String) getCurrentKey(),
                        active
                )));

            ++i;

            if (i == window.getSlicesNr()) // check if there where events received in the past v5minInSec
                state.value().addSlice(v5minInSec);

            curWindowEnd = state.value().getEndOfSlice(i);
        }

        // emit "watermark window" if not already done
        deltaWindowSum1 = 0.0;
        deltaWindowCount1 = 0;
        deltaWindowSum2 = 0.0;
        deltaWindowCount2 = 0;
        active = false;

        if (0 < i) { // use pre-aggregated results of previous windows if not already done
            preWindow1 = window.getP1Slice(i - 1);
            preWindow2 = window.getP2Slice(i - 1);

            deltaWindowSum1 = preWindow1.getWindowSum();
            deltaWindowCount1 = preWindow1.getWindowCount();
            deltaWindowSum2 = preWindow2.getWindowSum();
            deltaWindowCount2 = preWindow2.getWindowCount();

            startIdx = i - vDeltaIdx - 1;

            if (0 <= startIdx) { // check if there is a slice that we need to subtract
                preStart1 = window.getP1Slice(startIdx);
                preStart2 = window.getP2Slice(startIdx);

                deltaWindowSum1 -= preStart1.getSum();
                deltaWindowCount1 -= preStart1.getCount();
                deltaWindowSum2 -= preStart2.getSum();
                deltaWindowCount2 -= preStart2.getCount();

                long watermarkWindowStart = wm - v24hInSec;

                for (Event event : window.getP1Slice(startIdx).getEvents()) {
                    if (event.getTimestamp() < watermarkWindowStart) {
                        deltaWindowSum1 -= event.getValue();
                        --deltaWindowCount1;
                    }
                }

                for (Event event : window.getP2Slice(startIdx).getEvents()) {
                    if (event.getTimestamp() < watermarkWindowStart) {
                        deltaWindowSum2 -= event.getValue();
                        --deltaWindowCount2;
                    }
                }
            }

            // check if "watermark window" is active
            if (!window.getP1Slice(i - 1).isEmpty())
                active = true;

            if (1 < i && !window.getP1Slice(i - 2).isEmpty()) {
                long before10MinInSec = wm - v10minInSec;

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
                ++deltaWindowCount1;
            }
        }

        for (Event event : window.getP2Slice(i).getEvents()) {
            if (event.getTimestamp() < wm) {
                deltaWindowSum2 += event.getValue();
                ++deltaWindowCount2;
            }
        }

        if (!window.getP1Slice(i).isEmpty()) // check if window is active
            active = true;

        if (0.0 < deltaWindowCount1 && 0.0 < deltaWindowCount2) { // check if in last v24hInSec where tuples emitted
            float avgP1 = (float) deltaWindowSum1 / deltaWindowCount1;
            float avgP2 = (float) deltaWindowSum2 / deltaWindowCount2;

            output.collect(new StreamRecord<>(new AQIValue24h(
                    AQICalculator.getAQI(avgP2, avgP1),
                    wm,
                    (String) getCurrentKey(),
                    active
            )));
        }

        // remove slices that are already emitted and disjoint with all remaining windows that will be emitted
        state.value().removeSlices(wm - v24hInSec);

        // additionally remove all empty slices from tail
        state.value().removeEmptyTail();

        // clear state for this key if there are no slices anymore
        if (state.value().getSlicesNr() == 0) {
            state.clear();
        }
    }
}
