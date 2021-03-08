package de.hpi.debs;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MeasurementWatermarkGenerator implements WatermarkGenerator<MeasurementOwn> {
    @Override
    public void onEvent(MeasurementOwn event, long eventTimestamp, WatermarkOutput output) {
        if (event.isWatermark()) {
            output.emitWatermark(new Watermark(event.getTimestamp()));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

    }
}
