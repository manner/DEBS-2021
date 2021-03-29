package de.hpi.debs.slicing;

import java.util.ArrayList;

public class ParticleWindowState {
    protected ArrayList<Slice> slicesP1;
    protected ArrayList<Slice> slicesP2;
    protected long lastWatermark;
    protected int slicesNr;
    protected final String city;

    public ParticleWindowState(String city, long start, long step) {
        this.slicesP1 = new ArrayList<>();
        this.slicesP1.add(new Slice(start, start + step));
        this.slicesP2 = new ArrayList<>();
        this.slicesP2.add(new Slice(start, start + step));
        this.lastWatermark = Long.MAX_VALUE;
        this.slicesNr = 1;
        this.city = city;
    }

    public String getCity() {
        return city;
    }

    public long getLastWatermark() {
        return lastWatermark;
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

    public void removeSlice() throws Exception {
        if (slicesP1.isEmpty()) // debug only
            throw new Exception("there are no slices left");

        slicesP1.remove(0);
        slicesP2.remove(0);

        --slicesNr;
    }

    @Override
    public String toString() {
        return "ParticleWindowState{" +
                "slicesP1=" + slicesP1.toString() +
                ", slicesP2=" + slicesP2.toString() +
                ", lastWatermark=" + lastWatermark +
                ", slicesNr=" + slicesNr +
                ", city=" + city +
                '}';
    }
}
