package de.hpi.debs.aqi;

import de.hpi.debs.MeasurementOwn;
import de.hpi.debs.RollingSum;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AQIValueRollingPreProcessor extends KeyedProcessFunction<String, MeasurementOwn, AQIValue24h> {

    private ValueState<RollingSum> rollingP1;
    private ValueState<RollingSum> rollingP2;
    private long length = 86400 * 1000;

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public void open(Configuration parameters) {
        rollingP1 = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling1", RollingSum.class));
        rollingP2 = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling2", RollingSum.class));
    }

    @Override
    public void processElement(MeasurementOwn value, Context ctx, Collector<AQIValue24h> out) throws Exception {
        if(rollingP1.value() == null || rollingP2.value() == null) {
            rollingP1.update(new RollingSum(0.0, length));
            rollingP2.update(new RollingSum(0.0, length));
        }

        if(!value.getCity().equals("no")) {
            rollingP1.value().add(value.getP1(), value.getTimestamp());
            rollingP2.value().add(value.getP2(), value.getTimestamp());
        }

        if (value.isWatermark()) {
            // trigger average computation and emitting
            RollingSum rP1 = rollingP1.value();
            RollingSum rP2 = rollingP2.value();

            if (rP1.isEmpty() || rP2.isEmpty()) {
                out.collect(new AQIValue24h(
                        -1,
                        value.getTimestamp(),
                        true,
                        value.getCity()
                ));
            } else {
                float avgP1 = (float) rP1.trigger(value.getTimestamp());
                float avgP2 = (float) rP2.trigger(value.getTimestamp());

                out.collect(new AQIValue24h(
                        AQICalculator.getAQI(avgP2, avgP1),
                        value.getTimestamp(),
                        true,
                        value.getCity()
                ));
            }
        }
    }
}
