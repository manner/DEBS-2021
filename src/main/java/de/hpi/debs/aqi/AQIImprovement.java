package de.hpi.debs.aqi;

import java.util.Date;

public class AQIImprovement implements Comparable<AQIImprovement> {
    private double improvement;
    private int curAqiP1;
    private int curAqiP2;
    private long timestamp;
    private String city;
    private boolean isWatermark;

    public AQIImprovement(double improvement, int curAqiP1, int curAqiP2, long timestamp, String city, boolean isWatermark) {
        this.improvement = improvement;
        this.curAqiP1 = curAqiP1;
        this.curAqiP2 = curAqiP2;
        this.timestamp = timestamp;
        this.city = city;
        this.isWatermark = isWatermark;
    }

    public boolean isWatermark() {
        return isWatermark;
    }

    public void setWatermark(boolean watermark) {
        isWatermark = watermark;
    }

    public double getImprovement() {
        return improvement;
    }

    public void setImprovement(double improvement) {
        this.improvement = improvement;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "AQIImprovement{" +
                "improvement=" + improvement +
                ", timestamp=" + new Date(timestamp).toString() +
                ", city=" + city +
                '}';
    }

    @Override
    public int compareTo(AQIImprovement o) {
        return (int) Math.round(this.improvement - o.improvement);
    }
}
