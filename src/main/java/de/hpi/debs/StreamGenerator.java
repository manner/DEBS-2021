package de.hpi.debs;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import de.hpi.debs.serializer.BatchSerializer;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Measurement;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class StreamGenerator implements SourceFunction<MeasurementOwn> {
    private volatile boolean running = true;
    private int cnt = 0;
    private int batchNumbers;
    private Benchmark benchmark;
    private Optional<String> optionalCity;

    public StreamGenerator(
            Benchmark benchmarkIn,
            int batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;
    }

    public void processBatch(SourceContext<MeasurementOwn> context, Batch batch) {
        if (batch.getLast()) { // Stop when we get the last batch
            // System.out.println("Received last batch, finished!");
            running = false;
            return;
        }

        // process the batch of events we have
        List<Measurement> currentYearList = batch.getCurrentList();
        List<Measurement> lastYearList = batch.getLastyearList();

        Set<String> cities = new HashSet<>();

        for (Measurement measurement : currentYearList) {
            optionalCity = Main.locationRetriever.findCityForMeasurement(measurement);
            optionalCity.ifPresent(city -> {
                MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);
                cities.add(city);
                context.collectWithTimestamp(m, m.getTimestamp());
            });
        }

        // send watermarks for each city in batch
        for (String city : cities) {
            long watermarkTimestamp = currentYearList.get(currentYearList.size() - 1).getTimestamp().getSeconds() * 1000;
            MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp, city, true);
            context.collectWithTimestamp(watermark, watermarkTimestamp);
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
            return;
        }

        running = true;
    }

    @Override
    public void run(SourceContext<MeasurementOwn> context) {

        while (running) {
//            Batch batch = Main.challengeClient.nextBatch(benchmark);
            Batch batch = BatchSerializer.getBatch(Main.challengeClient, benchmark, cnt);

            processBatch(context, batch);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
