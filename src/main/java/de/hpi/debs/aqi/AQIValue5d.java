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

    public AQIValue5d(AQIValue24h event) {
        this.aqi = event.getAqi();
        this.curAqiP1 = event.getAqiP1();
        this.curAqiP2 = event.getAqiP2();
        this.timestamp = event.getTimestamp();
        this.watermark = event.isWatermark();
        this.city = event.getCity();
    }

    public double getAQI() {
        return aqi;
    }

    public int getCurAqiP1() {
        return curAqiP1;
    }

    public int getCurAqiP2() {
        return curAqiP2;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            AQIValue5d that = (AQIValue5d) o;
            return this.aqi == that.aqi
                    && this.curAqiP1 == that.curAqiP1
                    && this.curAqiP2 == that.curAqiP2
                    && this.timestamp == that.timestamp
                    && this.watermark == that.watermark
                    && this.city.equals(that.city);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "AQIValue5d{" +
                "AQI=" + aqi +
                ", curAqiP1=" + curAqiP1 +
                ", curAqiP2=" + curAqiP2 +
                ", timestamp=" + new Date(timestamp).toString() +
                ", city='" + city + '\'' +
                ", isWatermark='" + isWatermark() +
                '}';
    }
}
