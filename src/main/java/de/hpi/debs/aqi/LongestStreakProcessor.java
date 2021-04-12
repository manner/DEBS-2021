package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class LongestStreakProcessor extends KeyedProcessFunction<String, AQIValue24h, Streak> {
    class StreakState extends ArrayList<AQIValue24h> {
        protected long lastWatermark;
        protected Streak streak;

        public StreakState(long seq, String city) {
            super();

            lastWatermark = 0;
            streak = new Streak(seq, city);
        }

        @Override
        public boolean add(AQIValue24h value) {
            int index = this.size() - 1;

            if (index < 0)
                return super.add(value);

            while (0 <= index && value.getTimestamp() < this.get(index).getTimestamp()) {
                --index;
            }

            if (index < 0)
                index = 0;

            this.add(index, value);

            return true;
        }

        @Override
        public AQIValue24h remove(int index) {
            for (int i = 0; i < index; i++)
                super.remove(0);

            return null;
        }

        public void emitUntilWatermark(long wm, Collector<Streak> out) {
            int i = 0;
            int size = this.size();
            AQIValue24h aqi;

            if (i < size) {
                aqi = this.get(i);
            } else {
                lastWatermark = wm;
                return;
            }

            while (i < size && aqi.getTimestamp() <= wm) {
                streak.setTimestampLastMeasurement(aqi.getTimestamp());
                streak.updateSeq(aqi.getSeq());
                if (aqi.isGood()) {
                    if (streak.isBadStreak()) {
                        streak.startStreak(aqi.getTimestamp());
                    }
                } else {
                    streak.fail();
                }
                if (aqi.isWatermark()) {
                    out.collect(streak);
                    this.remove(i);
                }

                i++;
                aqi = this.get(i);
            }

            lastWatermark = wm;
        }

        public long getLastWatermark() {
            return lastWatermark;
        }
    }

    private ValueState<StreakState> state;

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("streakState", StreakState.class));
    }

    @Override
    public void processElement(AQIValue24h value, Context ctx, Collector<Streak> out) throws Exception {
        StreakState window = state.value();

        if (window != null && value.getTimestamp() < window.getLastWatermark()) // ignore late events
            return;

        if (window == null) {
            window = new StreakState(-1, ctx.getCurrentKey());
        }

        window.add(value);

        if (value.isWatermark())
            window.emitUntilWatermark(value.getTimestamp(), out);

        state.update(window);
    }
}
