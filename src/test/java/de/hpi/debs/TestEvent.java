package de.hpi.debs;

import de.tum.i13.bandency.Measurement;

public class TestEvent {
    private MeasurementOwn data;
    private long ts;
    private boolean watermark;

    public TestEvent(MeasurementOwn data, long ts, boolean watermark) {
        this.data = data;
        this.ts = ts;
        this.watermark = watermark;
    }

    public MeasurementOwn getData() {
        return data;
    }

    public long getTs() {
        return ts;
    }

    public boolean isWatermark() {
        return watermark;
    }
}
