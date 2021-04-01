package de.hpi.debs;

import de.hpi.debs.slicing.ParticleWindowState;
import de.hpi.debs.slicing.Slice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParticleWindowStateTest {

    public static class ParticleWindowStateTestClass extends ParticleWindowState {

        public ParticleWindowStateTestClass(String city, long start, long end) {
            super(city, start, end);
        }

        public ArrayList<Slice> getP1Slices() {
            return this.slicesP1;
        }

        public ArrayList<Slice> getP2Slices() {
            return this.slicesP2;
        }
    }

    long start = 0;
    long step = 300000;

    ArrayList<Slice> slices = new ArrayList<>() {{
        add((new Slice(start, start + step) {{
            add(1.0F, 3000);
            add(1.0F, 250000);
        }}));
        add((new Slice(start + step, start + 2 * step) {{
            add(1000.0F, 350000);
        }}));
        add((new Slice(start + 2 * step, start + 3 * step) {{
            add(1000.0F, 500000);
        }}));
        add((new Slice(start + 3 * step, start + 4 * step) {{
            add(1.0F, 800000);
        }}));
        add((new Slice(start + 4 * step, start + 5 * step) {{
            add(1.0F, 1100000);
        }}));
        add((new Slice(start + 5 * step, start + 6 * step) {{
            add(1.0F, 1400000);
        }}));
    }};

    @Test
    public void summingTest() {
        ParticleWindowStateTestClass pw = new ParticleWindowStateTestClass("city", 0, 300000);

        int i = 0;

        for (Slice s : slices) {
            for (Event e : s.getEvents())
                pw.addMeasure(i, (float) e.getValue(), (float) e.getValue(), e.getTimestamp());

            if (i < slices.size() - 1)
                pw.addSlice(300000L);
            ++i;
        }

        ArrayList<Slice> generatedP1 = pw.getP1Slices();
        i = 0;

        assertEquals(slices.size(), generatedP1.size());

        for (Slice s : generatedP1) {
            assertEquals(slices.get(i).getSum(), s.getSum());
            assertEquals(slices.get(i).getCount(), s.getCount());
            assertEquals(slices.get(i).getWindowSum(), s.getWindowSum());
            assertEquals(slices.get(i).getWindowCount(), s.getWindowCount());
            assertEquals(slices.get(i).getWindowAvg(), s.getWindowAvg());
            assertEquals(slices.get(i).getEnd(), s.getEnd());

            ++i;
        }

        ArrayList<Slice> generatedP2 = pw.getP2Slices();
        i = 0;

        assertEquals(slices.size(), generatedP2.size());

        for (Slice s : generatedP2) {
            assertEquals(slices.get(i).getSum(), s.getSum());
            assertEquals(slices.get(i).getCount(), s.getCount());
            assertEquals(slices.get(i).getWindowSum(), s.getWindowSum());
            assertEquals(slices.get(i).getWindowCount(), s.getWindowCount());
            assertEquals(slices.get(i).getWindowAvg(), s.getWindowAvg());
            assertEquals(slices.get(i).getEnd(), s.getEnd());

            ++i;
        }
    }

    ArrayList<Slice> slices2 = new ArrayList<>() {{
        add((new Slice(start, start + step) {{
            add(1.0F, 3000);
            add(1.0F, 250000);
        }}));
        add((new Slice(start + step, start + 2 * step) {{
            add(1000.0F, 350000);
            addToWindow(2.0F, 2);
        }}));
        add((new Slice(start + 2 * step, start + 3 * step) {{
            add(1000.0F, 500000);
            addToWindow(1002.0F, 3);
        }}));
        add((new Slice(start + 3 * step, start + 4 * step) {{
            add(1.0F, 800000);
            addToWindow(2002.0F, 4);
        }}));
        add((new Slice(start + 4 * step, start + 5 * step) {{
            add(1.0F, 1100000);
            addToWindow(2003.0F, 5);
        }}));
        add((new Slice(start + 5 * step, start + 6 * step) {{
            add(1.0F, 1400000);
            addToWindow(2004.0F, 6);
        }}));
    }};

    @Test
    public void preAggregateTest() {
        ParticleWindowStateTestClass pw = new ParticleWindowStateTestClass("city", 0, 300000);

        int i = 0;

        for (Slice s : slices2) {
            for (Event e : s.getEvents())
                pw.addMeasure(i, (float) e.getValue(), (float) e.getValue(), e.getTimestamp());

            if (i < slices2.size() - 1)
                pw.addSlice(step);

            if (0 < i)
                pw.addPreAggregate(
                        i,
                        pw.getP1Slice(i - 1).getWindowSum(),
                        pw.getP2Slice(i - 1).getWindowSum(),
                        pw.getP1Slice(i - 1).getWindowCount()
                );

            ++i;
        }

        ArrayList<Slice> generatedP1 = pw.getP1Slices();
        i = 0;

        assertEquals(slices2.size(), generatedP1.size());

        for (Slice s : generatedP1) {
            assertEquals(slices2.get(i).getSum(), s.getSum());
            assertEquals(slices2.get(i).getCount(), s.getCount());
            assertEquals(slices2.get(i).getWindowSum(), s.getWindowSum());
            assertEquals(slices2.get(i).getWindowCount(), s.getWindowCount());
            assertEquals(slices2.get(i).getWindowAvg(), s.getWindowAvg());
            assertEquals(slices2.get(i).getEnd(), s.getEnd());

            ++i;
        }

        ArrayList<Slice> generatedP2 = pw.getP2Slices();
        i = 0;

        assertEquals(slices2.size(), generatedP2.size());

        for (Slice s : generatedP2) {
            assertEquals(slices2.get(i).getSum(), s.getSum());
            assertEquals(slices2.get(i).getCount(), s.getCount());
            assertEquals(slices2.get(i).getWindowSum(), s.getWindowSum());
            assertEquals(slices2.get(i).getWindowCount(), s.getWindowCount());
            assertEquals(slices2.get(i).getWindowAvg(), s.getWindowAvg());
            assertEquals(slices2.get(i).getEnd(), s.getEnd());

            ++i;
        }
    }
}
