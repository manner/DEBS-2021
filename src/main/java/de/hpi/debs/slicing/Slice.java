package de.hpi.debs.slicing;

import de.hpi.debs.Event;

import java.util.ArrayList;

public class Slice {
    protected ArrayList<Event> events;
    protected double windowSum;
    protected int windowCount;
    protected double sum;
    protected int count;
    protected final long start;
    protected final long end; // end of window and slice

    public Slice(long start, long end) {
        this.events = new ArrayList<>();
        this.windowSum = 0.0;
        this.windowCount = 0;
        this.sum = 0.0;
        this.count = 0;
        this.start = start;
        this.end = end;
    }

    /**
     * Check if sum is empty.
     *
     * @return true if events list is empty and otherwise false.
     */
    public boolean isEmpty() {
        return events.isEmpty();
    }

    /**
     * Determines if event belongs to slice is in slice or not.
     *
     * @param ts Timestamp of the event.
     *
     * @return   0 - in slice
     *         < 0 - event time < slice start
     *         > 0 - slice end <= event time.
     */
    public int in(long ts) {
        if (ts < start)
            return -1;

        if (ts < end)
            return 0;

        return 1;
    }

    /**
     * Getter function of events in a slice.
     *
     * @return Slice events.
     */
    public ArrayList<Event> getEvents() {
        return events;
    }

    /**
     * Getter function of window and slice end.
     *
     * @return End of window and slice.
     */
    public long getEnd() {
        return end;
    }

    /**
     * Getter function of slice event sum.
     *
     * @return Sum of slice.
     */
    public double getSum() {
        return sum;
    }

    /**
     * Getter function of slice event count.
     *
     * @return Event count of slice.
     */
    public int getCount() {
        return count;
    }

    /**
     * Getter function of window event sum.
     *
     * @return Sum of window.
     */
    public double getWindowSum() {
        return windowSum;
    }

    /**
     * Getter function of window event count.
     *
     * @return Event count of window.
     */
    public int getWindowCount() {
        return windowCount;
    }

    /**
     * Getter function for average value of window.
     *
     * @return Average of window.
     */
    public double getWindowAvg() {
        return windowSum / windowCount;
    }

    /**
     * Adds elements to window.
     *
     * @param value Value of additional window sum.
     * @param count Count of additional window elements.
     */
    public void addToWindow(double value, int count) {
        windowSum += value;
        windowCount += count;
    }

    /**
     * Adds an element to slice and pre-aggregated window.
     *
     * @param value Value of the event.
     * @param timestamp Timestamp of the event.
     */
    public void add(double value, long timestamp) {
        events.add(new Event(value, timestamp));
        windowSum += value;
        ++windowCount;
        sum += value;
        ++count;
    }

    /**
     * Returns the object as a String.
     *
     * <p>This function adds an event to the rolling mean and updates sum and count.
     *
     * @return String containing values of the attributes.
     */
    @Override
    public String toString() {
        return "RollingSum{" +
                "events=" + events +
                ", sum=" + sum +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
