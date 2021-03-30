package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
            streakValueState.update(new Streak(aqiValue.getCity()));
        }

        if (aqiValue.isGood()) {
            if (streak.isBadStreak()) {
                streak.setTimestampSinceGoodAQI(aqiValue.getTimestamp());
            }
        } else {
            streak.fail();
        }

        streak.setTimestampLastMeasurement(aqiValue.getTimestamp());
        System.out.println(streak);
        streakValueState.update(streak);

        if (aqiValue.isWatermark()) {
            out.collect(streak);
        }
    }
//
//    public static class StreakMap {
//        private HashMap<String, Streak> streakMap;
//
//        public HashMap<String, Streak> getStreakMap() {
//            return streakMap;
//        }
//
//        public void put(String key, Streak streak) {
//            streakMap.put(key, streak);
//        }
//
//        public Streak get(String key) {
//            return streakMap.get(key);
//        }
//    }


    public static class Streak {
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

        public boolean isActive(long time) {
//            time - 10 min > lastMeasurement
            return true;
        }

        public void setTimestampSinceGoodAQI(Long timestamp) {
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
