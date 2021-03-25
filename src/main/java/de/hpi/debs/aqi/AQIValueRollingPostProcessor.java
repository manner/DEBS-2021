package de.hpi.debs.aqi;

import de.hpi.debs.RollingSum;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import de.hpi.debs.RollingSum;

public class AQIValueRollingPostProcessor
        extends KeyedProcessFunction<String, AQIValue24h, AQIValue5d> {

    private ValueState<RollingSum> rolling;

    @Override
    public void open(Configuration parameters) {
        rolling = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling", RollingSum.class));
    }

    @Override
    public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) throws Exception {
        if (rolling.value() == null) {
            rolling.update(new RollingSum(0.0, 432000 * 1000));
        }

        if(!value.getCity().equals("no"))
            rolling.value().add(value.getAQI(), value.getTimestamp());

        if (value.isWatermark()) {
            // trigger average computation and emitting
            double avgAQI = rolling.value().trigger(value.getTimestamp());

            out.collect(new AQIValue5d(avgAQI, value.getTimestamp(), true, value.getCity()));
        }
    }
}
