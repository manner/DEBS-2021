package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

import java.io.Serializable;
import java.util.Optional;

public class MeasurementOwn implements Serializable {
    private final float p1;
    private final float p2;

    private final float latitude;
    private final float longitude;

    private final float timestamp;

    private String city;

    public MeasurementOwn(float p1, float p2, float latitude, float longitude, float timestamp) {
        this.p1 = p1;
        this.p2 = p2;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
        this.city = null;
    }

    public static MeasurementOwn fromMeasurement(Measurement m) {
        return new MeasurementOwn(m.getP1(), m.getP2(), m.getLatitude(), m.getLongitude(), m.getTimestamp().getSeconds());
    }

    public Optional<String> getCity() {
        return Optional.ofNullable(city);
    }

    public void setCity(String city) {
        if (city != null) {
            this.city = city;
        }
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

    public float getTimestamp() {
        return timestamp;
    }
}
