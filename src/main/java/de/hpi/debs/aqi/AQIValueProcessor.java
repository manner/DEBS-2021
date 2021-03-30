package de.hpi.debs.aqi;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AQIValueProcessor extends ProcessWindowFunction<Integer, AQIValue24h, String, TimeWindow> {
    @Override
    public void process(
            String city,
            Context context,
            Iterable<Integer> elements,
            Collector<AQIValue24h> out
    ) {
        int AQI = elements.iterator().next();
        out.collect(new AQIValue24h(AQI, context.window().getEnd(), city, true));
    }
}
