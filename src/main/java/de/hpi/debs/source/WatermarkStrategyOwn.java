package de.hpi.debs.source;

import de.hpi.debs.MeasurementOwn;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.Watermark;

public class WatermarkStrategyOwn implements WatermarkStrategy<MeasurementOwn> {
    @Override
    public WatermarkGenerator<MeasurementOwn> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<>() {
            @Override
            public void onEvent(MeasurementOwn event, long eventTimestamp, WatermarkOutput output) {
                if (event.isLastWatermarkOfBatch())
                    output.emitWatermark(new Watermark(event.getTimestamp()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        };
    }
}
