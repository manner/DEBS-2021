package de.hpi.debs;

import java.util.Date;

public class Event {
    private final double value;
    private final long timestamp;

    public Event(double value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public double getValue() { return value; }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "value=" + value +
                ", timestamp=" + new Date(timestamp) +
                '}';
    }
}
