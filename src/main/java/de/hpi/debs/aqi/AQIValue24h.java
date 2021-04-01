package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue24h {
    private final int aqi;
    private final long timestamp;
    private final boolean watermark;
    private final String city;

    public AQIValue24h(int aqi, long timestamp, boolean watermark, String city) {
        this.aqi = aqi;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.city = city;
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
                    && this.city == null ? that.city == null : this.city.equals(that.city);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "AQIValue24h{" +
                "AQI=" + aqi +
                ", timestamp=" + new Date(timestamp).toString() +
                ", city='" + city + '\'' +
                ", isWatermark='" + isWatermark() +
                '}';
    }
}
