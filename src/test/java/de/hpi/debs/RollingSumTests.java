package de.hpi.debs;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RollingSumTests {

    public static class RollingSumTestClass extends RollingSum {

        public RollingSumTestClass(double sum, long length) { super(sum, length); }

        public ArrayList<Event> getEvents() { return events; }

        public double getSum() { return sum; }

        public int getCount() { return count; }

        public long getLength() { return length; }
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
    public void testSumming() {
        RollingSumTestClass rs = new RollingSumTestClass(0,1000);

        assertTrue(rs.getEvents().isEmpty());
        assertEquals(0.0, rs.getSum());
        assertEquals(0, rs.getCount());
        assertEquals(1000, rs.getLength());

        rs.add(events.get(0).getValue(), events.get(0).getTimestamp());
        rs.add(events.get(3).getValue(), events.get(3).getTimestamp());
        rs.add(events.get(8).getValue(), events.get(8).getTimestamp());

        assertEquals(
                events.get(0).getValue(),
                rs.getEvents().get(0).getValue()
        );
        assertEquals(
                events.get(0).getTimestamp(),
                rs.getEvents().get(0).getTimestamp()
        );
        assertEquals(
                events.get(3).getValue(),
                rs.getEvents().get(1).getValue()
        );
        assertEquals(
                events.get(3).getTimestamp(),
                rs.getEvents().get(1).getTimestamp()
        );
        assertEquals(
                events.get(8).getValue(),
                rs.getEvents().get(2).getValue()
        );
        assertEquals(
                events.get(8).getTimestamp(),
                rs.getEvents().get(2).getTimestamp()
        );
        assertEquals(30.0, rs.getSum());
        assertEquals(3, rs.getCount());
        assertEquals(1000, rs.getLength());
    }

    @Test
    public void testTrigger() {
        RollingSumTestClass rs = new RollingSumTestClass(0, 1000);

        for (Event event : events)
            rs.add(event.getValue(), event.getTimestamp());

        double avg = 0;

        for (int i = 3; i < 8; i++)
            avg += events.get(i).getValue();

        avg /= 5.0;

        assertEquals(avg, rs.trigger(1611));
        assertEquals(70.0, rs.getSum());
        assertEquals(7, rs.getCount());
        assertEquals(1000, rs.getLength());

        for (int i = 0; i < 7; i++) {

            assertEquals(
                    events.get(i + 3).getValue(),
                    rs.getEvents().get(i).getValue()
            );
            assertEquals(events.get(i + 3).getTimestamp(), rs.getEvents().get(i).getTimestamp());
        }
    }
}
