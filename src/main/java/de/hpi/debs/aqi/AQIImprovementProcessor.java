package de.hpi.debs.aqi;

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class AQIImprovementProcessor extends ProcessJoinFunction<AQIValue5d, AQIValue5d, AQIImprovement> {
    @Override
    public void processElement(AQIValue5d currentYear, AQIValue5d lastYear, Context ctx, Collector<AQIImprovement> out) {
        double improvement = lastYear.getAQI() - currentYear.getAQI();
        out.collect(new AQIImprovement(
                improvement,
                currentYear.getCurAqiP1(),
                currentYear.getCurAqiP2(),
                currentYear.getTimestamp(),
                currentYear.getCity(),
                currentYear.isWatermark()
        ));
    }
}