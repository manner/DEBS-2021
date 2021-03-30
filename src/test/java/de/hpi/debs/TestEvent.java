package de.hpi.debs;

public class TestEvent {
    private MeasurementOwn data;
    private long ts;

    public TestEvent(MeasurementOwn data, long ts) {
        this.data = data;
        this.ts = ts;
    }

    public MeasurementOwn getData() {
        return data;
    }

    public long getTs() {
        return ts;
    }
}
