package de.hpi.debs;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import de.hpi.debs.serializer.BatchSerializer;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Measurement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StreamGenerator implements SourceFunction<MeasurementOwn> {
    private volatile boolean running = true;
    private int cnt = 0;
    private int batchNumbers;
    private Benchmark benchmark;
    private Optional<String> optionalCity;
    private HashMap<String, Long> cities;


    public StreamGenerator(
            Benchmark benchmarkIn,
            int batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;
        cities = new HashMap<>();
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
        Measurement lastMeasurement = currentYearList.get(currentYearList.size() - 1);


        for (Measurement measurement : currentYearList) {
            optionalCity = Main.locationRetriever.findCityForMeasurement(measurement);
            optionalCity.ifPresent(city -> {
                MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);

                Long lastTimestamp = cities.get(city);
                if (lastTimestamp == null) {
                    cities.putIfAbsent(city, m.getTimestamp());
                } else if (m.getTimestamp() > lastTimestamp) {
                    cities.put(city, m.getTimestamp());
                }
                context.collectWithTimestamp(m, m.getTimestamp());
            });
        }

        // send watermarks for each city in batch
        long watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000 + lastMeasurement.getTimestamp().getNanos() / 1000;
        for (Map.Entry<String, Long> city : cities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp, city.getKey(), true);
                context.collectWithTimestamp(watermark, watermarkTimestamp);
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
        context.emitWatermark(new Watermark(watermarkTimestamp));

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
