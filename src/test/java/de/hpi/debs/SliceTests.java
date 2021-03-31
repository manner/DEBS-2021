package de.hpi.debs;

import de.hpi.debs.slicing.Slice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SliceTests {

    public static class SliceTestClass extends Slice {

        public SliceTestClass(long start, long end) {
            super(start, end);
        }

        public ArrayList<Event> getEvents() {
            return events;
        }

        public long getLength() {
            return end - start;
        }
    }

    protected ArrayList<Event> events = new ArrayList<>() {{
        add(new Event(10, 99));
        add(new Event(10, 200));
        add(new Event(10, 300));
        add(new Event(10, 611));
        add(new Event(10, 1501));
        add(new Event(10, 1601));
        add(new Event(10, 1001));
        add(new Event(10, 1551));
        add(new Event(10, 1611));
        add(new Event(10, 2001));
    }};

    @Test
    public void summingTest() {
        SliceTestClass rs = new SliceTestClass(0, 1000);

        assertTrue(rs.getEvents().isEmpty());
        assertEquals(0.0, rs.getSum());
        assertEquals(0, rs.getCount());
        assertEquals(1000, rs.getLength());

        rs.add(events.get(0).getValue(), events.get(0).getTimestamp());
        rs.add(events.get(3).getValue(), events.get(3).getTimestamp());
        rs.add(events.get(8).getValue(), events.get(8).getTimestamp());

        assertEquals(events.get(0).getValue(), rs.getEvents().get(0).getValue());
        assertEquals(events.get(0).getTimestamp(), rs.getEvents().get(0).getTimestamp());
        assertEquals(events.get(3).getValue(), rs.getEvents().get(1).getValue());
        assertEquals(events.get(3).getTimestamp(), rs.getEvents().get(1).getTimestamp());
        assertEquals(events.get(8).getValue(), rs.getEvents().get(2).getValue());
        assertEquals(events.get(8).getTimestamp(), rs.getEvents().get(2).getTimestamp());
        assertEquals(30.0, rs.getSum());
        assertEquals(3, rs.getCount());
        assertEquals(30.0, rs.getWindowSum());
        assertEquals(3, rs.getWindowCount());
        assertEquals(1000, rs.getLength());
    }

    @Test
    public void windowAverageTest() {
        SliceTestClass rs = new SliceTestClass(0, 1000);

        for (Event event : events)
            rs.add(event.getValue(), event.getTimestamp());

        double avg = 0;

        for (int i = 0; i < 10; i++)
            avg += events.get(i).getValue();

        avg /= 10.0;

        assertEquals(100.0, rs.getSum());
        assertEquals(10, rs.getCount());
        assertEquals(100.0, rs.getWindowSum());
        assertEquals(10, rs.getWindowCount());
        assertEquals(avg, rs.getWindowAvg());
        assertEquals(1000, rs.getLength());

        for (int i = 0; i < 10; i++) {

            assertEquals(
                    events.get(i).getValue(),
                    rs.getEvents().get(i).getValue()
            );
            assertEquals(events.get(i).getTimestamp(), rs.getEvents().get(i).getTimestamp());
        }
    }

    @Test
    public void addToWindowTest() {
        SliceTestClass rs = new SliceTestClass(0, 1000);

        rs.add(10, 0);
        rs.add(10, 3);
        rs.addToWindow(1000, 10);

        assertEquals(20, rs.getSum());
        assertEquals(2, rs.getCount());
        assertEquals(1020, rs.getWindowSum());
        assertEquals(12, rs.getWindowCount());
        assertEquals(1020.0 / 12.0, rs.getWindowAvg());
        assertEquals(1000, rs.getLength());

        assertEquals(10, rs.getEvents().get(0).getValue());
        assertEquals(0, rs.getEvents().get(0).getTimestamp());
        assertEquals(10, rs.getEvents().get(1).getValue());
        assertEquals(3, rs.getEvents().get(1).getTimestamp());
    }
}
