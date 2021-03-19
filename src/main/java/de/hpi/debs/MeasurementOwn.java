package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

import java.io.Serializable;

public class MeasurementOwn implements Serializable {
    private final float p1;
    private final float p2;

    private final float latitude;
    private final float longitude;

    private final long timestamp;

    private String city;

    private boolean isWatermark;

    public MeasurementOwn(float p1, float p2, float latitude, float longitude, long timestamp, String city) {
        this.p1 = p1;
        this.p2 = p2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.city = city;
        this.isWatermark = false;
    }

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
                m.getTimestamp().getSeconds(),
                city);
    }

    public static MeasurementOwn fromMeasurement(Measurement m, String city, boolean watermark) {
        return new MeasurementOwn(
                m.getP1(),
                m.getP2(),
                m.getLatitude(),
                m.getLongitude(),
                m.getTimestamp().getSeconds(),
                city,
                watermark);
    }

    public boolean isWatermark() {
        return isWatermark;
    }

    public void setWatermark(boolean value) {
        this.isWatermark = value;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) { this.city = city; }

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
}
