package de.hpi.debs.slicing;

import de.hpi.debs.aqi.AQIValue5d;

import java.util.ArrayList;

public class WatermarkedBuckets {

    ArrayList<AQIValue5d> elements;
    long watermark; // or end time stamp of bucket

    public WatermarkedBuckets() {
        elements = new ArrayList<>();
        watermark = -1L;
    }

    public void setWatermark(long wm) {
        watermark = wm;
    }

    public long getWatermark() {
        return watermark;
    }

    public void addElement(AQIValue5d element) {
        elements.add(element);
    }

    public ArrayList<AQIValue5d> getElements() {
        return elements;
    }
}
