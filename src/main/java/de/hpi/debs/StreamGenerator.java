package de.hpi.debs;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import de.hpi.debs.serializer.BatchSerializer;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Measurement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class StreamGenerator implements SourceFunction<MeasurementOwn> {
    private volatile boolean running = true;
    private int cnt = 0;
    private int batchNumbers;
    private Benchmark benchmark;

    public StreamGenerator(
            Benchmark benchmarkIn,
            int batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;
    }

    @Override
    public void run(SourceContext<MeasurementOwn> context) {

        Optional<String> optionalCity;

        while (running) {
//            Batch batch = Main.challengeClient.nextBatch(benchmark);
            Batch batch = BatchSerializer.getBatch(Main.challengeClient, benchmark, cnt);

            if (batch.getLast()) { // Stop when we get the last batch
                // System.out.println("Received last batch, finished!");
                running = false;
                break;
            }

            // process the batch of events we have
            List<Measurement> currentYearList = batch.getCurrentList();
            List<Measurement> lastYearList = batch.getLastyearList();

            HashMap<String, List<MeasurementOwn>> currentMap = new HashMap<>();

            for (Measurement measurement : currentYearList) {
                optionalCity = Main.locationRetriever.findCityForMeasurement(measurement);
                optionalCity.ifPresent(city -> {
                    MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);
                    currentMap.computeIfAbsent(city, a -> new ArrayList<>());
                    currentMap.get(city).add(m);
                });
            }

            for (List<MeasurementOwn> measurementsForCity : currentMap.values()) {
                measurementsForCity.sort((m1, m2) -> (int) (m1.getTimestamp() - m2.getTimestamp()));
                for (int i = 0; i < measurementsForCity.size(); i++) {
                    MeasurementOwn m = measurementsForCity.get(i);
                    if (i == measurementsForCity.size() - 1) {
                        m.setIsWatermark();
                    }
                    context.collectWithTimestamp(m, m.getTimestamp());
                }
            }

            for (Measurement measurement : lastYearList) {
                optionalCity = Main.locationRetriever.findCityForMeasurement(measurement);
                optionalCity.ifPresent(city -> {
                    MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);
                    context.collectWithTimestamp(m, m.getTimestamp());
                });
            }

            // emit flink watermark
            context.emitWatermark(new Watermark(currentYearList.get(currentYearList.size() - 1).getTimestamp().getSeconds() * 1000));

            ++cnt;
            if (cnt >= batchNumbers) { //for testing you can
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
