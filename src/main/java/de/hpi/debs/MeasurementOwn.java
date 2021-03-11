package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

import java.io.Serializable;
import java.util.Optional;

public class MeasurementOwn implements Serializable {
    private final float p1;
    private final float p2;

    private final float latitude;
    private final float longitude;

    private final long timestamp;

    private String city;

    private boolean isWatermark;

    public MeasurementOwn(float p1, float p2, float latitude, float longitude, long timestamp) {
        this.p1 = p1;
        this.p2 = p2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.city = null;
        this.isWatermark = false;
    }

    public static MeasurementOwn fromMeasurement(Measurement m) {
        return new MeasurementOwn(m.getP1(), m.getP2(), m.getLatitude(), m.getLongitude(), m.getTimestamp().getSeconds());
    }

    public boolean isWatermark() {
        return isWatermark;
    }

    public void setWatermark(boolean value) {
        this.isWatermark = value;
    }

    public Optional<String> getCity() {
        return city != null ? Optional.of(city) : Optional.empty();
    }

    public void setCity(Optional<String> city) {
        city.ifPresent(c -> this.city = c);
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
}
