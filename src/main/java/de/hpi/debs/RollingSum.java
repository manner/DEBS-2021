package de.hpi.debs;

import java.util.ArrayList;

public class RollingSum {
    protected ArrayList<Event> events;
    protected float sum;
    protected int count;
    protected long length;

    public RollingSum(long length) {
        this.events = new ArrayList<>();
        this.sum = 0;
        this.count = 0;
        this.length = length;
    }

    /**
     * Adds an element to the rolling sum.
     *
     * @param value Value of the event.
     * @param timestamp Timestamp of the event.
     */
    public void add(double value, long timestamp) {
        events.add(new Event(value, timestamp));
        sum += value;
        ++count;
    }

    /**
     * Triggers computing average and cleans the rolling sum.
     *
     * <p>The Trigger computes the rolling average for the end timestamp and drops events that are out of the window.
     *
     * @param watermark End time of the rolling sum.
     * @return The average value of the window.
     */
    public float trigger(long watermark) {
        float sumAvg = sum;
        int countAvg = count;
        long start = watermark - length;

        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).getTimestamp() < start) {// Remove elements that are out of the window.
                sum -= events.get(i).getValue();
                --count;
                events.remove(i);
                --i;
            } else if (events.get(i).getTimestamp() >= watermark) {// Skip events that are ahead the watermark.
                sumAvg -= events.get(i).getValue();
                --countAvg;
            }
        }

        return sumAvg / countAvg;
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
                "sum=" + sum +
                ", count=" + count +
                ", length=" + length +
                '}';
    }
}
