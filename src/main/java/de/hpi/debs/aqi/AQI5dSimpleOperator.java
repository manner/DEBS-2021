package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class AQI5dSimpleOperator extends KeyedProcessOperator<String, AQIValue24h, AQIValue5d> {
    private ListState<AQIValue24h> aqiValues;

    public AQI5dSimpleOperator() {
        super(new KeyedProcessFunction<>() {
            @Override
            public void processElement(AQIValue24h value, Context ctx, Collector<AQIValue5d> out) {
                // do nothing as we are doing everything in the operator
            }
        });
    }

    @Override
    public void open() throws Exception {
        ListStateDescriptor<AQIValue24h> descriptor =
                new ListStateDescriptor<>(
                        "aqiValues",
                        AQIValue24h.class);
        aqiValues = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(StreamRecord<AQIValue24h> value) throws Exception {
        AQIValue24h aqi = value.getValue();

        aqiValues.add(aqi);

        // emit when internal watermark
        if (aqi.isWatermark()) {
            AQIValue5d aqiValue5d = calculateAverageFor5dBefore(aqi.getTimestamp());
            if (aqiValue5d != null) {
                output.collect(new StreamRecord<>(aqiValue5d, aqiValue5d.getTimestamp()));
            }
        }
    }

    private AQIValue5d calculateAverageFor5dBefore(long end) throws Exception {
        long start = end - Time.days(5).toMilliseconds();
        List<AQIValue24h> newAQIs = new ArrayList<>();
        AQIValue24h lastAqi = null;

        long sumAQI = 0;
        long counter = 0;

        for (AQIValue24h aqi : aqiValues.get()) {
            // AQI values more than 5 days ago
            if (aqi.getTimestamp() < start) {
                continue;
            }
            newAQIs.add(aqi);

            // AQI values that are in the future shouldn't count towards average
            if (aqi.getTimestamp() > end) {
                continue;
            }

            lastAqi = aqi;
            counter++;
            sumAQI += aqi.getAqi();
        }

        // Discard measurements older than 5d
        aqiValues.update(newAQIs);

        if (counter > 0) {
            float avgAQI = (float) sumAQI / counter;
            return new AQIValue5d(
                    lastAqi.getSeq(),
                    avgAQI,
                    lastAqi.getAqiP1(),
                    lastAqi.getAqiP2(),
                    end,
                    true,
                    lastAqi.getCity());
        }
        return null;
    }
}