package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue5d {
    private final double aqi;
    private final long timestamp;
    private final boolean watermark;
    private final String city;

    public AQIValue5d(double aqi, long timestamp, boolean watermark, String city) {
        this.aqi = aqi;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.city = city;
    }

    public double getAQI() {
        return aqi;
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

    @Override
    public String toString() {
        return "AQIValue{" +
                "AQI=" + aqi +
                ", timestamp=" + new Date(timestamp * 1000) +
                ", city='" + city + '\'' +
                '}';
    }
}
