package de.hpi.debs.slicing;

import java.util.ArrayList;

public class AqiWindowState {
    protected ArrayList<Slice> slicesAqi;
    protected long lastWatermark;
    protected int slicesNr;
    protected int checkpoint;
    protected final String city;
    protected double eventsSum;
    protected int executionMode; // 0 - pre-aggregated window slicing, 1 - slice pre-aggregation only & no window pre-aggregation

    public AqiWindowState(String city, long start, long end) {
        this.slicesAqi = new ArrayList<>();
        this.slicesAqi.add(new Slice(start, end));
        this.lastWatermark = start;
        this.slicesNr = 1;
        this.checkpoint = 0;
        this.city = city;
        this.eventsSum = 0.0;
        this.executionMode = 0;
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

    public Slice getAqiSlice(int index) {
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

        slicesAqi.add(new Slice(lastEnd, lastEnd + step));

        ++slicesNr;
    }

    public void addMeasure(int index, double aqi, long ts) {
        slicesAqi.get(index).add(aqi, ts);

        ++eventsSum;
    }

    public void addPreAggregate(int index, double sumAqi, int count) {
        slicesAqi.get(index).addToWindow(sumAqi, count);

        ++checkpoint;

        eventsSum += count;
    }

    public void removeSlices(long ts) {
        while (!slicesAqi.isEmpty() && slicesAqi.get(0).getEnd() <= ts) {
            eventsSum -= slicesAqi.get(0).getSum();

            slicesAqi.remove(0);

            --slicesNr;
        }
    }

    public void removeEmptyTail() {
        while (!slicesAqi.isEmpty() && slicesAqi.get(0).isEmpty()) {
            eventsSum -= slicesAqi.get(0).getSum();

            slicesAqi.remove(0);

            --slicesNr;
        }
    }

    @Override
    public String toString() {
        return "ParticleWindowState{" +
                "slicesAQI=" + slicesAqi +
                ", lastWatermark=" + lastWatermark +
                ", slicesNr=" + slicesNr +
                ", city=" + city +
                ", eventsSum=" + eventsSum +
                ", executionMode=" + executionMode +
                '}';
    }
}