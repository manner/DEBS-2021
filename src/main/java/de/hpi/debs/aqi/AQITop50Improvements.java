package de.hpi.debs.aqi;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.stream.StreamSupport;

public class AQITop50Improvements extends ProcessWindowFunction<AQIImprovement, AQIImprovement, Long, TimeWindow> {
    private static final int limit = 50;

    @Override
    public void process(Long aLong, Context context, Iterable<AQIImprovement> elements, Collector<AQIImprovement> out) throws Exception {
        StreamSupport.stream(elements.spliterator(), false)
                .sorted(Comparator.comparingDouble(AQIImprovement::getImprovement).reversed())
                .limit(limit)
                .forEach(out::collect);

    }
}
