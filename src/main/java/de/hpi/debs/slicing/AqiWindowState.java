package de.hpi.debs.slicing;

import java.util.ArrayList;

public class AqiWindowState {
    protected ArrayList<AqiSlice> slicesAqi;
    protected long lastWatermark;
    protected int slicesNr;
    protected int checkpoint;
    protected final String city;
    protected int executionMode; // 0 - pre-aggregated window slicing, 1 - slice pre-aggregation only & no window pre-aggregation

    public AqiWindowState(String city, long start, long end) {
        this.slicesAqi = new ArrayList<>();
        this.slicesAqi.add(new AqiSlice(start, end));
        this.lastWatermark = start;
        this.slicesNr = 1;
        this.checkpoint = 0;
        this.city = city;
        this.executionMode = 0;
    }

    public int getCheckpoint() {
        return checkpoint;
    }

    public String getCity() {
        return city;
    }

    public long getLastWatermark() {
        return lastWatermark;
    }

    public void updateLastWatermark(long lw) {
        lastWatermark = lw;
    }

    public int getSlicesNr() {
        return slicesNr;
    }

    public long getEndOfSlice(int index) {
        return slicesAqi.get(index).getEnd();
    }

    public AqiSlice getAqiSlice(int index) {
        return slicesAqi.get(index);
    }

    public int in(int sliceIdx, long ts) {
        return slicesAqi.get(sliceIdx).in(ts);
    }

    public boolean preAggregate(int index) {
        return checkpoint < index;
    }

    public void addSlice(long step) {
        long lastEnd = slicesAqi.get(slicesNr - 1).getEnd();

        slicesAqi.add(new AqiSlice(lastEnd, lastEnd + step));

        ++slicesNr;
    }

    public void addMeasure(int index, double aqi, int aqiP1, int aqiP2, long ts) {
        slicesAqi.get(index).add(aqi, aqiP1, aqiP2, ts);
    }

    public void addPreAggregate(int index, double sumAqi, int count) {
        slicesAqi.get(index).addToWindow(sumAqi, count);

        ++checkpoint;
    }

    public void removeSlices(long ts) {
        while (!slicesAqi.isEmpty() && slicesAqi.get(0).getEnd() <= ts) {
            slicesAqi.remove(0);

            --slicesNr;
            --checkpoint;
        }
    }

    public void removeEmptyTail() {
        while (!slicesAqi.isEmpty() && slicesAqi.get(0).isEmpty()) {
            slicesAqi.remove(0);

            --slicesNr;
            --checkpoint;
        }
    }

    @Override
    public String toString() {
        return "ParticleWindowState{" +
                "slicesAQI=" + slicesAqi +
                ", lastWatermark=" + lastWatermark +
                ", slicesNr=" + slicesNr +
                ", city=" + city +
                ", executionMode=" + executionMode +
                '}';
    }
}