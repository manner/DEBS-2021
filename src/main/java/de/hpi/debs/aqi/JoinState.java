package de.hpi.debs.aqi;

import de.hpi.debs.slicing.WatermarkedBuckets;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;

public class JoinState {

    protected ArrayList<WatermarkedBuckets> elements1;
    protected int nextWmIndex1;
    protected long lastWm1;
    protected ArrayList<WatermarkedBuckets> elements2;
    protected int nextWmIndex2;
    protected long lastWm2;
    protected final long OFFSET;

    public JoinState(long offset) {
        elements1 = new ArrayList<>();
        elements1.add(new WatermarkedBuckets());
        nextWmIndex1 = 0;
        lastWm1 = 0L;
        elements2 = new ArrayList<>();
        elements2.add(new WatermarkedBuckets());
        nextWmIndex2 = 0;
        lastWm2 = 0L;
        OFFSET = offset;
    }

    public void addElement1(AQIValue5d aqi) {
        if (aqi.getTimestamp() <= lastWm1) // late events
            return;

        if (aqi.isWatermark()) {
            elements1.get(nextWmIndex1).addElement(aqi);
            elements1.get(nextWmIndex1).setWatermark(aqi.getTimestamp());
            elements1.add(new WatermarkedBuckets());
            lastWm1 = aqi.getTimestamp();
            ++nextWmIndex1;
        }

        if (0 < nextWmIndex1 && aqi.getTimestamp() <= elements1.get(nextWmIndex1 - 1).getWatermark()) {
            elements1.get(nextWmIndex1 - 1).addElement(aqi);
        } else {
            elements1.get(nextWmIndex1).addElement(aqi);
        }
    }

    public void addElement2(AQIValue5d aqi) {
        if (aqi.getTimestamp() <= lastWm2) // late events
            return;

        aqi.offsetTimestamp(OFFSET);

        if (aqi.isWatermark()) {
            elements2.get(nextWmIndex2).addElement(aqi);
            elements2.get(nextWmIndex2).setWatermark(aqi.getTimestamp());
            elements2.add(new WatermarkedBuckets());
            lastWm2 = aqi.getTimestamp();
            ++nextWmIndex2;
        }

        if (0 < nextWmIndex2 && aqi.getTimestamp() <= elements2.get(nextWmIndex2 - 1).getWatermark()) {
            elements2.get(nextWmIndex2 - 1).addElement(aqi);
        } else {
            elements2.get(nextWmIndex2).addElement(aqi);
        }
    }

    public void emit(Output<StreamRecord<AQIImprovement>> out) {
        if (nextWmIndex1 <= 0 || nextWmIndex2 <= 0) // we need data from both streams otherwise there is no improvement
            return;

        double improvement;
        long curWm;
        long lastWm;

        for (int current = 0; current < nextWmIndex1; current++) {
            curWm = elements1.get(current).getWatermark();

            for (int last = 0; last < nextWmIndex2; last++) {
                lastWm = elements2.get(last).getWatermark();

                if (curWm <= lastWm) { // emit improvements when applicable
                    for (AQIValue5d curItem : elements1.get(current).getElements()) {
                        for (AQIValue5d lastItem : elements2.get(last).getElements()) {
                            improvement = lastItem.getAQI() - curItem.getAQI();

                            out.collect(new StreamRecord<>(
                                    new AQIImprovement(
                                            improvement,
                                            curItem.getCurAqiP1(),
                                            curItem.getCurAqiP2(),
                                            curItem.getTimestamp(),
                                            curItem.getCity(),
                                            curItem.isWatermark()
                                    ),
                                    curItem.getTimestamp()
                            ));
                        }
                    }
                }

                if (curWm <= lastWm) {
                    elements1.remove(current); // emitted already all matching events
                    --nextWmIndex1;
                    --current;
                } else { //  if (lastWm <= curWm)
                    elements2.remove(last); // emitted already all matching events
                    --nextWmIndex2;
                    --last;
                }
            }
        }
    }
}
