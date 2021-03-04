package de.hpi.debs;

public class PointOwn {
    private final float latitude;
    private final float longitude;

    public PointOwn(float latitude, float longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public float getLatitude() {
        return latitude;
    }
}
