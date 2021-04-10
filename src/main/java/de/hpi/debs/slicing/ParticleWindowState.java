package de.hpi.debs.slicing;

import java.util.ArrayList;

public class ParticleWindowState {
    protected ArrayList<Slice> slicesP1;
    protected ArrayList<Slice> slicesP2;
    protected long lastWatermark;
    protected int slicesNr;
    protected int checkpoint;
    protected final String city;
    protected int executionMode; // 0 - pre-aggregated window slicing, 1 - slice pre-aggregation only & no window pre-aggregation

    public ParticleWindowState(String city, long start, long end) {
        this.slicesP1 = new ArrayList<>();
        this.slicesP1.add(new Slice(start, end));
        this.slicesP2 = new ArrayList<>();
        this.slicesP2.add(new Slice(start, end));
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
        return slicesP1.get(index).getEnd();
    }

    public Slice getP1Slice(int index) {
        return slicesP1.get(index);
    }

    public Slice getP2Slice(int index) {
        return slicesP2.get(index);
    }

    public int in(int sliceIdx, long ts) {
        return slicesP1.get(sliceIdx).in(ts);
    }

    public boolean preAggregate(int index) {
        return checkpoint < index;
    }

    public void addSlice(long step) {
        long lastEnd = slicesP1.get(slicesNr - 1).getEnd();

        slicesP1.add(new Slice(lastEnd, lastEnd + step));
        slicesP2.add(new Slice(lastEnd, lastEnd + step));

        ++slicesNr;
    }

    public void addMeasure(int index, float p1, float p2, long ts) {
        slicesP1.get(index).add(p1, ts);
        slicesP2.get(index).add(p2, ts);
    }

    public void addPreAggregate(int index, double sumP1, double sumP2, int count) {
        slicesP1.get(index).addToWindow(sumP1, count);
        slicesP2.get(index).addToWindow(sumP2, count);

        ++checkpoint;
    }

    public void removeSlices(long ts) {
        while (!slicesP1.isEmpty() && slicesP1.get(0).getEnd() <= ts) {
            slicesP1.remove(0);
            slicesP2.remove(0);

            --slicesNr;
            --checkpoint;
        }
    }

    public void removeEmptyTail() {
        while (!slicesP1.isEmpty() && slicesP1.get(0).isEmpty()) {
            slicesP1.remove(0);
            slicesP2.remove(0);

            --slicesNr;
            --checkpoint;
        }
    }

    @Override
    public String toString() {
        return "ParticleWindowState{" +
                "slicesP1=" + slicesP1.toString() +
                ", slicesP2=" + slicesP2.toString() +
                ", lastWatermark=" + lastWatermark +
                ", slicesNr=" + slicesNr +
                ", checkpoint=" + checkpoint +
                ", city=" + city +
                ", executionMode=" + executionMode +
                '}';
    }
}