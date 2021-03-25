package de.hpi.debs;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import de.hpi.debs.aqi.AQIImprovement;

public class WatermarkTrigger extends Trigger<AQIImprovement, GlobalWindow> {
    @Override
    public TriggerResult onElement(AQIImprovement element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
        return element.isWatermark() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {

    }
}
