package de.hpi.debs.aqi;

import de.hpi.debs.slicing.AqiWindowState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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
    boolean addOneYear;

    public AQIValue5dProcessOperator(long start) {
        super(
                new KeyedProcessFunction<>() {
                    @Override
                    public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) {
                        // do nothing as we are doing everything in the operator
                    }
                }
        );

        this.addOneYear = false;
        this.start = start;
        this.size = 432000000;
        this.step = 300000;
        this.doubleStep = 2 * step;
        this.vDeltaIdx = (int)(size / step) - 1;
    }

    public AQIValue5dProcessOperator(long start, boolean addOneYear) {
        super(
            new KeyedProcessFunction<>() {
                @Override
                public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) {
                    // do nothing as we are doing everything in the operator
                }
            }
        );

        this.addOneYear = addOneYear;
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
    public void open() throws Exception {
        super.open();
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", AqiWindowState.class));
    }

    @Override
    public void processElement(StreamRecord<AQIValue24h> value) throws Exception {
        AqiWindowState window = state.value();

        if (window != null && value.getTimestamp() < window.getLastWatermark()) // ignore late events
            return;

        if (value.getValue().isWatermark()) { // emit results on watermark arrival
            if (window == null) { // in case no records are processed beforehand watermark has all values

                output.collect(new StreamRecord<>(new AQIValue5d(value.getValue()), value.getTimestamp()));
                return;
            }

            long wm = value.getTimestamp();
            long lw = window.getLastWatermark();
            window.updateLastWatermark(wm);

            double deltaWindowSum;
            int deltaWindowCount;
            int startIdx;
            long curWindowEnd;

            // go to first slice that need to emit window result
            int i = window.getCheckpoint();

            curWindowEnd = window.getEndOfSlice(i);

            // emit results of pending windows
            while (curWindowEnd <= wm) {
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
                }

                if (!window.getAqiSlice(i).isEmpty() && lw < curWindowEnd) {
                    double avgAqi = window.getAqiSlice(i).getWindowAvg();
                    int curAqiP1 = window.getAqiSlice(i).getAqiP1s().get(0);
                    int curAqiP2 = window.getAqiSlice(i).getAqiP2s().get(0);

                    output.collect(new StreamRecord<>(new AQIValue5d(
                            value.getValue().getSeq(),
                            avgAqi,
                            curAqiP1,
                            curAqiP2,
                            curWindowEnd,
                            false,
                            (String) getCurrentKey()),
                            curWindowEnd
                    ));
                }

                ++i;

                if (i == window.getSlicesNr()) // check if there where events received in the past v5minInSec
                    window.addSlice(step);

                curWindowEnd = window.getEndOfSlice(i);
            }

            // emit watermark with current 24h averages
            output.collect(new StreamRecord<>(new AQIValue5d(
                    value.getValue().getSeq(),
                    value.getValue().getAqi(),
                    value.getValue().getAqiP1(),
                    value.getValue().getAqiP2(),
                    wm,
                    true,
                    value.getValue().getCity()),
                    wm
            ));

            // remove slices that are already emitted and disjoint with all remaining windows that will be emitted
            window.removeSlices(wm - size);

            // additionally remove all empty slices from tail
            window.removeEmptyTail();

            // clear state for this key if there are no slices anymore
            if (window.getSlicesNr() == 0) {
                state.clear();
            } else {
                state.update(window);
            }

            return;
        }

        if (window == null) {
            long newStart = value.getTimestamp() - size;
            window = new AqiWindowState(
                    (String) getCurrentKey(),
                    newStart,
                    value.getTimestamp()
            );
        }

        // add tuple that actually has a matching city
        int i = window.getSlicesNr() - 1;
        int in = window.in(i, value.getTimestamp());

        // search for correct slice
        while (in != 0) {
            if (window.getAqiSlice(i).getEnd() == value.getTimestamp()) // as AQIValue24h slices are mapped to AQIValue5d slices with same end
                break;
            if (in < 0) { // go one slice to the past
                --i;
            } else {
                window.addSlice(step); // add new slice to end

                ++i;
            }

            in = window.in(i, value.getTimestamp());
        }

        // update slice by new event
        window.addMeasure(
                i,
                value.getValue().getAqi(),
                value.getValue().getAqiP1(),
                value.getValue().getAqiP2(),
                value.getTimestamp()
        );

        state.update(window);
    }
}