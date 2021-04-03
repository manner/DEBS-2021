package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Optional;

public class LongestStreakProcessor extends KeyedProcessFunction<String, AQIValue24h, LongestStreakProcessor.Streak> {
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

    public static class Streak implements Serializable {
        private long timestampLastMeasurement;
        private Optional<Long> timestampSinceGoodAQI;
        private String city;

        public Streak(String city) {
            this.city = city;
            this.timestampSinceGoodAQI = Optional.empty();
        }

        public void setTimestampLastMeasurement(long timestampLastMeasurement) {
            this.timestampLastMeasurement = timestampLastMeasurement;
        }

        @Override
        public String toString() {
            return "Streak{" +
                    "timestampLastMeasurement=" + timestampLastMeasurement +
                    ", timestampSinceGoodAQI=" + timestampSinceGoodAQI +
                    ", city='" + city + '\'' +
                    '}';
        }

        public Integer getBucket(long watermarkTimestamp, int bucketSize) {
            return timestampSinceGoodAQI
                    .map(ts -> {
                                long streakInMs = watermarkTimestamp - ts;
                                long streak = Math.min(streakInMs, Time.days(7).toMilliseconds());
                                return (int) (Math.floor((float) streak / bucketSize));
                            }
                    )
                    .orElse(0);
        }

        public void startStreak(Long timestamp) {
            timestampSinceGoodAQI = Optional.of(timestamp);
        }

        public boolean isBadStreak() {
            return timestampSinceGoodAQI.isEmpty();
        }

        public void fail() {
            timestampSinceGoodAQI = Optional.empty();
        }
    }
}
