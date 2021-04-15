package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import de.hpi.debs.MeasurementOwn;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class AQI24hSimpleOperator extends KeyedProcessOperator<String, MeasurementOwn, AQIValue24h> {
    private final static long FIVE_MINUTES = Time.minutes(5).toMilliseconds();
    private final long currentStart = LocalDateTime.of(2020, Month.MARCH, 1, 0, 0).toEpochSecond(ZoneOffset.UTC) * 1000;
    private ListState<MeasurementOwn> measurements;
    private ValueState<Long> snapshotTimeValue;

    public AQI24hSimpleOperator() {
        super(new KeyedProcessFunction<>() {
            @Override
            public void processElement(MeasurementOwn value, Context ctx, Collector<AQIValue24h> out) {
                // do nothing as we are doing everything in the operator
            }
        });
    }

    @Override
    public void open() throws Exception {
        ListStateDescriptor<MeasurementOwn> descriptor =
                new ListStateDescriptor<>(
                        "measurements",
                        MeasurementOwn.class);
        measurements = getRuntimeContext().getListState(descriptor);

        snapshotTimeValue = getRuntimeContext().getState(new ValueStateDescriptor<>("snapshotTime", Long.class, currentStart + FIVE_MINUTES));
    }

    @Override
    public void processElement(StreamRecord<MeasurementOwn> value) throws Exception {
        MeasurementOwn measurement = value.getValue();
        long snapshotTime = snapshotTimeValue.value();

        // emit when 5min snapshot has passed
        if (measurement.getTimestamp() > snapshotTime) {
            AQIValue24h aqiValue24h = calculateAQIFor24hBefore(snapshotTime, false);
            if (aqiValue24h != null) {
                output.collect(new StreamRecord<>(aqiValue24h, snapshotTime));
            }
            snapshotTimeValue.update(snapshotTime + FIVE_MINUTES);
        }

        measurements.add(measurement);

        // emit when internal watermark
        if (measurement.isWatermark()) {
            AQIValue24h aqiValue24h = calculateAQIFor24hBefore(measurement.getTimestamp(), true);
            output.collect(new StreamRecord<>(aqiValue24h, measurement.getTimestamp()));
        }
    }

    private AQIValue24h calculateAQIFor24hBefore(long end, boolean isWatermark) throws Exception {
        long start = end - Time.hours(24).toMilliseconds();
        List<MeasurementOwn> newMeasurements = new ArrayList<>();
        MeasurementOwn lastMeasurement = null;
        long sumAQI = 0;
        long counter = 0;

        for (MeasurementOwn measurement : measurements.get()) {
            // Measurement more than 24 hours ago
            if (measurement.getTimestamp() < start) {
                continue;
            }
            newMeasurements.add(measurement);
            // measurements that are in the future shouldn't count towards average
            if (measurement.getTimestamp() > end) {
                continue;
            }

            if (!measurement.isWatermark()) {
                lastMeasurement = measurement;
                counter++;
                sumAQI += AQICalculator.getAQI(measurement);
            }
        }

        // Discard measurements older than 24h
        measurements.update(newMeasurements);

        if (counter > 0) {
            int aqiValue = Math.round((float) sumAQI / counter);
            return new AQIValue24h(
                    lastMeasurement.getSeq(),
                    aqiValue,
                    AQICalculator.getAQI10(lastMeasurement.getP1()),
                    AQICalculator.getAQI25(lastMeasurement.getP2()),
                    end,
                    isWatermark,
                    lastMeasurement.getCity());
        }
        return null;
    }
}