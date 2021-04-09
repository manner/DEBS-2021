package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue24h {
    private final long seq;
    private final int aqi;
    private final int aqiP1;
    private final int aqiP2;
    private final long timestamp;
    private final boolean watermark;
    private final String city;

    public AQIValue24h(long seq, int aqi, int aqiP1, int aqiP2, long timestamp, boolean watermark, String city) {
        this.seq = seq;
        this.aqi = aqi;
        this.aqiP1 = aqiP1;
        this.aqiP2 = aqiP2;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.city = city;
    }

    public long getSeq() {
        return seq;
    }

    public int getAqi() {
        return aqi;
    }

    public int getAqiP1() {
        return aqiP1;
    }

    public int getAqiP2() {
        return aqiP2;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isWatermark() {
        return watermark;
    }

    public String getCity() {
        return city;
    }

    public boolean isGood() {
        return aqi <= 50;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            AQIValue24h that = (AQIValue24h) o;
            return this.aqi == that.aqi
                    && this.timestamp == that.timestamp
                    && this.watermark == that.watermark
                    && this.city.equals(that.city);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "AQIValue24h{" +
                "AQI=" + aqi +
                ", aqiP1=" + aqiP1 +
                ", aqiP2=" + aqiP2 +
                ", timestamp=" + new Date(timestamp) +
                ", city='" + city + '\'' +
                ", isWatermark='" + isWatermark() +
                '}';
    }
}
