package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Optional;

public class LongestStreakProcessor extends KeyedProcessFunction<String, AQIValue24h, Streak> {
    private ValueState<Streak> streakValueState;

    @Override
    public void open(Configuration parameters) {
        streakValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("streak", Streak.class));
    }

    @Override
    public void processElement(AQIValue24h aqiValue, Context ctx, Collector<Streak> out) throws Exception {
        Streak streak = streakValueState.value();

        if (streak == null) {
            streak = new Streak(aqiValue.getCity());
            streakValueState.update(streak);
        }

        if (aqiValue.isGood()) {
            if (streak.isBadStreak()) {
                streak.startStreak(aqiValue.getTimestamp());
            }
        } else {
            streak.fail();
        }

        streak.setTimestampLastMeasurement(aqiValue.getTimestamp());
        streakValueState.update(streak);

        if (aqiValue.isWatermark()) {
            out.collect(streak);
        }
    }
}
