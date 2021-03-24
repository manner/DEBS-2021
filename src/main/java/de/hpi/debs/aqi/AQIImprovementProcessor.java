package de.hpi.debs.aqi;

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class AQIImprovementProcessor extends ProcessJoinFunction<AQIValue5d, AQIValue5d, AQIImprovement> {
    @Override
    public void processElement(AQIValue5d left, AQIValue5d right, Context ctx, Collector<AQIImprovement> out) throws Exception {
        // TODO
    }
}
