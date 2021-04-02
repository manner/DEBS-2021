package de.hpi.debs;

import akka.io.SelectionHandlerSettings;
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
    private final int batchNumbers;
    private final Benchmark benchmark;
    private final HashMap<String, Long> cities;
    private final HashMap<String, Long> lastYearCities;


    public StreamGenerator(
            Benchmark benchmarkIn,
            int batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;
        cities = new HashMap<>();
        lastYearCities = new HashMap<>();
    }

    public void processBatch(SourceContext<MeasurementOwn> context, Batch batch) {
        // process the batch of events we have
        List<Measurement> currentYearList = batch.getCurrentList();
        List<Measurement> lastYearList = batch.getLastyearList();
        Measurement lastMeasurement;
        long watermarkTimestamp;
        if (currentYearList.isEmpty()) {
            lastMeasurement = lastYearList.get(lastYearList.size() - 1);
            watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000;
            watermarkTimestamp += lastMeasurement.getTimestamp().getNanos() / 1000;
            watermarkTimestamp += 31536000000L;
        } else {
            lastMeasurement = currentYearList.get(currentYearList.size() - 1);
            watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000;
            watermarkTimestamp += lastMeasurement.getTimestamp().getNanos() / 1000;
        }

        Optional<String> optionalCity;

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

        for (Measurement measurement : lastYearList) {
            optionalCity = Main.locationRetriever.findCityForMeasurement(measurement);
            optionalCity.ifPresent(city -> {
                MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);

                Long lastTimestamp = lastYearCities.get(city);
                if (lastTimestamp == null) {
                    lastYearCities.putIfAbsent(city, m.getTimestamp());
                } else if (m.getTimestamp() > lastTimestamp) {
                    lastYearCities.put(city, m.getTimestamp());
                }
                context.collectWithTimestamp(m, m.getTimestamp());
            });
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : cities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp, city.getKey(), true);
                context.collectWithTimestamp(watermark, watermarkTimestamp);
            }
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : lastYearCities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - 31536000000L - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp - 31536000000L, city.getKey(), true);
                context.collectWithTimestamp(watermark, watermarkTimestamp - 31536000000L);
            }
        }

        // emit flink watermark
        context.emitWatermark(new Watermark(watermarkTimestamp));

        ++cnt;
        if (cnt >= batchNumbers) { //for testing you can
            running = false;
            return;
        }

        if (batch.getLast()) { // Stop when we get the last batch
            // System.out.println("Received last batch, finished!");
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
