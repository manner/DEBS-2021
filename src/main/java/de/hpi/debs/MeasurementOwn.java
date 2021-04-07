package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class MeasurementOwn implements Serializable {

    private final float p1;
    private final float p2;

    private final float latitude;
    private final float longitude;

    private final long timestamp;
    private boolean isWatermark;
    private String city;
    private final boolean isLastYear;

    public MeasurementOwn(float p1, float p2, float latitude, float longitude, long timestamp, String city, boolean watermark, boolean isLastYear) {
        this.p1 = p1;
        this.p2 = p2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.city = city;
        this.isWatermark = watermark;
        this.isLastYear = isLastYear;
    }

    public static MeasurementOwn fromMeasurement(Measurement m, String city) {
        return new MeasurementOwn(
                m.getP1(),
                m.getP2(),
                m.getLatitude(),
                m.getLongitude(),
                m.getTimestamp().getSeconds() * 1000 + m.getTimestamp().getNanos() / 1000,
                city,
                false,
                false);
    }

    public static MeasurementOwn fromMeasurement(Measurement m, String city, long addToTS, boolean isLastYear) {
        return new MeasurementOwn(
                m.getP1(),
                m.getP2(),
                m.getLatitude(),
                m.getLongitude(),
                m.getTimestamp().getSeconds() * 1000 + m.getTimestamp().getNanos() / 1000 + addToTS,
                city,
                false,
                isLastYear);
    }

    public static MeasurementOwn createWatermark(long timestamp, String city) {
        return new MeasurementOwn(
                0,
                0,
                0,
                0,
                timestamp,
                city,
                true,
                false
        );
    }

    public static List<MeasurementOwn> fromMeasurements(List<Measurement> measurementList) {
        return measurementList.stream()
                .map(m -> fromMeasurement(m, "testCity"))
                .collect(Collectors.toList());
    }

    public boolean isLastYear() {
        return isLastYear;
    }

    public boolean isCurrentYear() {
        return !isLastYear;
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

    public long getTimestamp() {
        return timestamp;
    }

    public LocalDateTime getLocalDateTimeStamp() {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), TimeZone.getTimeZone("GMT").toZoneId());
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && getClass() == o.getClass()) {
            MeasurementOwn that = (MeasurementOwn) o;
            return this.p1 == that.p1
                    && this.p2 == that.p2
                    && this.latitude == that.latitude
                    && this.longitude == that.longitude
                    && this.timestamp == that.timestamp
                    && this.isWatermark == that.isWatermark
                    && this.city.equals(that.city);
        } else {
            return false;
        }
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
