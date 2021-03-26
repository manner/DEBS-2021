package de.hpi.debs.aqi;

import de.hpi.debs.slicing.Slice;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AQIValueRollingPostProcessor
        extends KeyedProcessFunction<String, AQIValue24h, AQIValue5d> {

    private ValueState<Slice> rolling;
    private final long length = 432000;

    @Override
    public void open(Configuration parameters) {
        rolling = getRuntimeContext().getState(new ValueStateDescriptor<>("rolling", Slice.class));
    }

    @Override
    public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) throws Exception {
        if(rolling.value() == null)
            rolling.update(new Slice(value.getTimestamp(), value.getTimestamp() + length));

        if(!value.getCity().equals("no"))
            rolling.value().add(value.getAQI(), value.getTimestamp());
    }
}
