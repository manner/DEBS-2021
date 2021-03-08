package de.hpi.debs;

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
}
