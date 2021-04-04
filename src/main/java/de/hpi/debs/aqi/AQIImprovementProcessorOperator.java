package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

public class AQIImprovementProcessorOperator
        extends AbstractUdfStreamOperator<AQIImprovement, ProcessJoinFunction<AQIValue5d, AQIValue5d, AQIImprovement>>
        implements TwoInputStreamOperator<AQIValue5d, AQIValue5d, AQIImprovement>, Triggerable<String, String> {

    protected ValueState<JoinState> state;

    public AQIImprovementProcessorOperator() {
        super(new ProcessJoinFunction<>() {
            @Override
            public void processElement(AQIValue5d left, AQIValue5d right, Context ctx, Collector<AQIImprovement> out) {
                // do nothing as we do everything in the Operator
            }
        });
    }

    @Override
    public void open() throws Exception {
        super.open();
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("state", JoinState.class));
    }

    @Override
    public void onEventTime(InternalTimer<String, String> timer) {
        // do nothing as we trigger on internal watermark arrival
    }

    @Override
    public void onProcessingTime(InternalTimer<String, String> timer) {
        // do nothing as we trigger on internal watermark arrival
    }

    @Override
    public void processElement1(StreamRecord<AQIValue5d> element) throws Exception {
        if (state.value() == null) {
            state.update(new JoinState(Time.days(365).toMilliseconds()));
        }

        state.value().addElement1(element.getValue());
        state.value().emit(output);
    }

    @Override
    public void processElement2(StreamRecord<AQIValue5d> element) throws Exception {
        if (state.value() == null) {
            state.update(new JoinState(Time.days(365).toMilliseconds()));
        }

        state.value().addElement2(element.getValue());
    }
}
