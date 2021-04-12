package de.hpi.debs.aqi;

import org.apache.flink.api.common.time.Time;

import java.io.Serializable;
import java.util.Optional;

public class Streak implements Serializable {
    private long seq;
    private long timestampLastMeasurement;
    private Optional<Long> timestampSinceGoodAQI;
    private String city;

    public Streak(long seq, String city) {
        this.seq = seq;
        this.city = city;
        this.timestampSinceGoodAQI = Optional.empty();
    }

    public void setTimestampLastMeasurement(long timestampLastMeasurement) {
        this.timestampLastMeasurement = timestampLastMeasurement;
    }

    public long getSeq() {
        return seq;
    }

    public long getTimestampLastMeasurement() {
        return timestampLastMeasurement;
    }

    public void updateSeq(long seq) {
        this.seq = seq;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            Streak that = (Streak) o;
            return this.timestampLastMeasurement == that.timestampLastMeasurement
                    && this.timestampSinceGoodAQI.equals(that.timestampSinceGoodAQI)
                    && this.city.equals(that.city);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Streak{" +
                "seq=" + seq +
                ", timestampLastMeasurement=" + timestampLastMeasurement +
                ", timestampSinceGoodAQI=" + timestampSinceGoodAQI +
                ", city='" + city + "'" +
                "}";
    }
}
