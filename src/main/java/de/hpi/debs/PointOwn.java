package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

public class PointOwn {
    private final float latitude;
    private final float longitude;

    public PointOwn(float latitude, float longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public PointOwn(Measurement m) {
        this.latitude = m.getLatitude();
        this.longitude = m.getLongitude();
    }

    public float getLongitude() {
        return longitude;
    }

    public float getLatitude() {
        return latitude;
    }
}
