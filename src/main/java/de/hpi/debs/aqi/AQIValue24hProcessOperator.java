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

import java.util.concurrent.ExecutionException;

public class AQIValue24hProcessOperator extends KeyedProcessOperator<String, MeasurementOwn, AQIValue24h> {

    private ValueState<ParticleWindowState> state;

    public final long start;
    public static final long v24hInSec = 86400;
    public static final long v5minInSec = 300;
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
        if (state.value() == null)
            state.update(new ParticleWindowState((String) getCurrentKey(), start, start + v5minInSec));

        if (state.value().getLastWatermark() < value.getTimestamp()) // ignore late events
            return;

        // add tuple that actually has a matching city
        int index = state.value().getSlicesNr() - 1;
        int in = state.value().in(index, value.getTimestamp());

        // search for correct slice
        while (in != 0) {
            if (in < 0) { // go one slice to the past
                --index;
                if (index < 0) // debug only
                    throw new Exception("removed one slice in the past that I seem to need again");
            } else { // add new slice to end
                state.value().addSlice(v5minInSec);

                ++index;
            }

            in = state.value().in(index, value.getTimestamp());
        }

        // update slice by new event
        state.value().addMeasure(index, value.getValue().getP1(), value.getValue().getP2(), value.getTimestamp());
    }

    @Override
    public void processWatermark(Watermark watermark) throws Exception {
        long wm = watermark.getTimestamp();
        long lWatermark = state.value().getLastWatermark();
        String curCity = (String)getCurrentKey();

        ParticleWindowState window = state.value();

        if (window.getSlicesNr() < 0) { // window has no particle measures yet
            output.collect(new StreamRecord<>(new AQIValue24h(
                    -1, // empty list should be returned
                    wm,
                    curCity
            )));

            return;
        }

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

        // emit windows that need to be emitted
        int i = window.getSlicesNr() - 1;

        // go to slice that need to emit a window first
        while (lWatermark < window.getEndOfSlice(i)) {
            --i;
            if (i < 0) // debug only
                throw new Exception("run to negative slice");
        }

        curWindowEnd = window.getEndOfSlice(i);

        if (i == 0 && curWindowEnd < wm) { // only avg from first slice will be emitted
            float avgP1 = (float) window.getP1Slice(i).getWindowAvg();
            float avgP2 = (float) window.getP2Slice(i).getWindowAvg();

            output.collect(new StreamRecord<>(new AQIValue24h(
                    AQICalculator.getAQI(avgP2, avgP1),
                    curWindowEnd,
                    curCity
            )));

            ++i;

            curWindowEnd = window.getEndOfSlice(i);
        }

        for (; curWindowEnd < wm; i++) {
            // pre-aggregate window
            preWindow1 = window.getP1Slice(i - 1);
            preWindow2 = window.getP2Slice(i - 1);

            deltaWindowSum1 = preWindow1.getWindowSum();
            deltaWindowCount1 = preWindow1.getWindowCount();
            deltaWindowSum2 = preWindow2.getWindowSum();
            deltaWindowCount2 = preWindow2.getWindowCount();

            startIdx = i - vDeltaIdx - 1;

            if (0 <= startIdx) { // check if we exited initial phase, in the initial phase consecutive windows are sum of all previous windows
                preStart1 = window.getP1Slice(startIdx);
                preStart2 = window.getP2Slice(startIdx);

                deltaWindowSum1 -= preStart1.getSum();
                deltaWindowCount1 -= preStart1.getCount();
                deltaWindowSum2 -= preStart2.getSum();
                deltaWindowCount2 -= preStart2.getCount();
            }

            try {
                window.getP1Slice(i).addToWindow(deltaWindowSum1, deltaWindowCount1);
                window.getP2Slice(i).addToWindow(deltaWindowSum2, deltaWindowCount2);
            } catch (Exception e) {
                e.printStackTrace();
            }

            float avgP1 = (float) window.getP1Slice(i).getWindowAvg();
            float avgP2 = (float) window.getP2Slice(i).getWindowAvg();

            output.collect(new StreamRecord<>(new AQIValue24h(
                    AQICalculator.getAQI(avgP2, avgP1),
                    wm,
                    (String)getCurrentKey()
            )));

            curWindowEnd = state.value().getEndOfSlice(i);
        }

        // emit the "watermark window"
        long watermarkWindowStart = wm - v24hInSec;
        deltaWindowSum1 = 0.0;
        deltaWindowCount1 = 0;
        deltaWindowSum2 = 0.0;
        deltaWindowCount2 = 0;

        if (0 < i) { // when we have a previous window that was emitted use it's aggregate
            // pre-aggregate window
            preWindow1 = window.getP1Slice(i - 1);
            preWindow2 = window.getP2Slice(i - 1);

            deltaWindowSum1 = preWindow1.getWindowSum();
            deltaWindowCount1 = preWindow1.getWindowCount();
            deltaWindowSum2 = preWindow2.getWindowSum();
            deltaWindowCount2 = preWindow2.getWindowCount();

            startIdx = i - vDeltaIdx - 1;

            if (0 <= startIdx) { // check if we exited initial phase, in the initial phase consecutive windows are sum of all previous windows
                // first remove events from first slice from window
                preStart1 = window.getP1Slice(startIdx);
                preStart2 = window.getP2Slice(startIdx);

                deltaWindowSum1 -= preStart1.getSum();
                deltaWindowCount1 -= preStart1.getCount();
                deltaWindowSum2 -= preStart2.getSum();
                deltaWindowCount2 -= preStart2.getCount();

                // then add events from first slice that belong to "watermark" window
                for (Event event : window.getP1Slice(i).getEvents()) {
                    if (watermarkWindowStart <= event.getTimestamp()) {
                        deltaWindowSum1 += event.getValue();
                        ++deltaWindowCount1;
                    }
                }

                for (Event event : window.getP2Slice(i).getEvents()) {
                    if (watermarkWindowStart <= event.getTimestamp()) {
                        deltaWindowSum2 += event.getValue();
                        ++deltaWindowCount2;
                    }
                }

            }
        }

        // add events from last slice that belong to "watermark" window
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

        float avgP1 = (float) deltaWindowSum1 / deltaWindowCount1;
        float avgP2 = (float) deltaWindowSum1 / deltaWindowCount2;

        output.collect(new StreamRecord<>(new AQIValue24h(
                AQICalculator.getAQI(avgP2, avgP1),
                wm,
                (String)getCurrentKey()
        )));

        // remove first slices that are already emitted and disjoint with all remaining windows that will be emitted in future
        startIdx = i - vDeltaIdx - 1; // remove all slice that have smaller index

        for (i = 0; i < startIdx; i++)
            state.value().removeSlice();
    }
}
