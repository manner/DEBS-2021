package de.hpi.debs.aqi;

import java.util.Date;

public class AQIValue24h {
    private final int aqi;
    private final long timestamp;
    private final String city;

    public AQIValue24h(int aqi, long timestamp, String city) {
        this.aqi = aqi;
        this.timestamp = timestamp;
        this.city = city;
    }

    public int getAQI() {
        return aqi;
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
                "AQI=" + aqi +
                ", timestamp=" + new Date(timestamp * 1000) +
                ", city='" + city + '\'' +
                '}';
    }
}
