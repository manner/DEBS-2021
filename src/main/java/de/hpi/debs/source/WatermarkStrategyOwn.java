package de.hpi.debs.source;

import de.tum.i13.bandency.Batch;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class WatermarkStrategyOwn implements WatermarkStrategy<Batch> {
    @Override
    public WatermarkGenerator<Batch> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<>() {
            @Override
            public void onEvent(Batch event, long eventTimestamp, WatermarkOutput output) {

            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {

            }
        };
    }
}
