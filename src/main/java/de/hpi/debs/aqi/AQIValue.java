package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue {
    private final int AQI;
    private final long timestamp;
    private final String city;

    public AQIValue(int aqi, long timestamp, String city) {
        this.AQI = aqi;
        this.timestamp = timestamp;
        this.city = city;
    }

    public int getAQI() {
        return AQI;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getCity() {
        return city;
    }

    @Override
    public String toString() {
        return "AQIValue{" +
                "AQI=" + AQI +
                ", timestamp=" + new Date(timestamp * 1000) +
                ", city='" + city + '\'' +
                '}';
    }
}
