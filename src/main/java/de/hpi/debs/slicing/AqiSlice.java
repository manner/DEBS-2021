package de.hpi.debs.slicing;

import de.hpi.debs.Event;

import java.util.ArrayList;

public class AqiSlice extends Slice {
    protected ArrayList<Integer> aqiP1s; // current AQI values of slices
    protected ArrayList<Integer> aqiP2s;

    public AqiSlice(long start, long end) {
        super(start, end);

        this.aqiP1s = new ArrayList<>();
        this.aqiP2s = new ArrayList<>();
    }

    /**
     * Getter function of aqi p1 values in a slice.
     *
     * @return Aqi p1 values.
     */
    public ArrayList<Integer> getAqiP1s() {
        return aqiP1s;
    }

    /**
     * Getter function of aqi p2 values in a slice.
     *
     * @return Aqi p2 values.
     */
    public ArrayList<Integer> getAqiP2s() {
        return aqiP2s;
    }

    /**
     * Adds an element to slice and pre-aggregated window.
     *
     * @param value Value of the event.
     * @param timestamp Timestamp of the event.
     */
    @Override
    public void add(double value, long timestamp) {
        // is not supported
    }

    /**
     * Adds an element to slice and pre-aggregated window.
     *
     * @param averageAqi Value of the event.
     * @param AqiP1 Value of the event.
     * @param AqiP2 Value of the event.
     * @param timestamp Timestamp of the event.
     */
    public void add(double averageAqi, int AqiP1, int AqiP2, long timestamp) {
        events.add(new Event(averageAqi, timestamp));
        aqiP1s.add(AqiP1); // dummy values
        aqiP2s.add(AqiP2);
        windowSum += averageAqi;
        ++windowCount;
        sum += averageAqi;
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
        return "Slice{" +
                "events=" + events +
                ", aqiP1s=" + aqiP1s +
                ", aqiP2s=" + aqiP2s +
                ", sum=" + sum +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
