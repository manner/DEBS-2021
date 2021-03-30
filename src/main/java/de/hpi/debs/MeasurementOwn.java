package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.TimeZone;

public class MeasurementOwn implements Serializable {

    private static final LocalDateTime FIRST_OF_2020 = LocalDateTime.of(2020, Month.JANUARY, 1, 0, 0);
    private final float p1;
    private final float p2;

    private final float latitude;
    private final float longitude;

    private final long timestamp;
    private final boolean isWatermark;
    private String city;

    public MeasurementOwn(float p1, float p2, float latitude, float longitude, long timestamp, String city, boolean watermark) {
        this.p1 = p1;
        this.p2 = p2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.city = city;
        this.isWatermark = watermark;
    }

    public static MeasurementOwn fromMeasurement(Measurement m, String city) {
        return new MeasurementOwn(
                m.getP1(),
                m.getP2(),
                m.getLatitude(),
                m.getLongitude(),
                m.getTimestamp().getSeconds() * 1000,
                city,
                false);
    }

    public static MeasurementOwn fromMeasurement(Measurement m, String city, boolean watermark) {
        return new MeasurementOwn(
                m.getP1(),
                m.getP2(),
                m.getLatitude(),
                m.getLongitude(),
                m.getTimestamp().getSeconds() * 1000,
                city,
                watermark);
    }

    public boolean isLastYear() {
        return getLocalDateTimeStamp().isBefore(FIRST_OF_2020);
    }

    public boolean isCurrentYear() {
        return !isLastYear();
    }

    public boolean isWatermark() {
        return isWatermark;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public float getP1() {
        return p1;
    }

    public float getP2() {
        return p2;
    }

    public PointOwn getPoint() {
        return new PointOwn(latitude, longitude);
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getLocalDateTimeStamp() {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone.getTimeZone("GMT").toZoneId());
    }

    @Override
    public String toString() {
        return "MeasurementOwn{" +
                (isLastYear() ? "lastYear" : "currentYear") +
                ", p1=" + p1 +
                ", p2=" + p2 +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", timestamp=" + getLocalDateTimeStamp().toString() +
                ", isWatermark=" + isWatermark +
                ", city='" + city + '\'' +
                '}';
    }
}
