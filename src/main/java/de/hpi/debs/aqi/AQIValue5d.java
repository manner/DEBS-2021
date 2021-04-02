package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue5d {
    private final double aqi;
    private final int curAqiP1;
    private final int curAqiP2;
    private final long timestamp;
    private final boolean watermark;
    private final String city;

    public AQIValue5d(double aqi, int aqiP1, int aqiP2, long timestamp, boolean watermark, String city) {
        this.aqi = aqi;
        this.curAqiP1 = aqiP1;
        this.curAqiP2 = aqiP2;
        this.timestamp = timestamp;
        this.watermark = watermark;
        this.city = city;
    }

    public double getAQI() {
        return aqi;
    }

    public double getCurAqiP1() {
        return aqi;
    }

    public double getCurAqiP2() {
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
        return "AQIValue5d{" +
                "AQI=" + aqi +
                ", timestamp=" + new Date(timestamp).toString() +
                ", city='" + city + '\'' +
                ", isWatermark='" + isWatermark() +
                '}';
    }
}
