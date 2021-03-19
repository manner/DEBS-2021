package de.hpi.debs.aqi;

import de.hpi.debs.MeasurementOwn;
import de.hpi.debs.RollingSum;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AQIValueRollingProcessor extends KeyedProcessFunction<String, MeasurementOwn, AQIValue> {

    private ValueState<RollingSum> rollingP1;
    private ValueState<RollingSum> rollingP2;

    @Override
    public void open(Configuration parameters) {
        rollingP1 = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", RollingSum.class));
        rollingP2 = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", RollingSum.class));
    }

    @Override
    public void processElement(MeasurementOwn value, Context ctx, Collector<AQIValue> out) throws Exception {
        if(rollingP1.value() == null || rollingP2.value() == null) {
            rollingP1.update(new RollingSum(86400));
            rollingP2.update(new RollingSum(86400));
        }

        if(!value.getCity().equals("no")) {
            rollingP1.value().add(value.getP1(), value.getTimestamp());
            rollingP2.value().add(value.getP2(), value.getTimestamp());
        }

        if (value.isWatermark()) {
            // trigger average computation and emitting
            float avgP1 = rollingP1.value().trigger(value.getTimestamp());
            float avgP2 = rollingP2.value().trigger(value.getTimestamp());

            out.collect(new AQIValue(AQICalculator.getAQI(avgP2, avgP1), value.getTimestamp(), value.getCity()));
        }
    }
}
