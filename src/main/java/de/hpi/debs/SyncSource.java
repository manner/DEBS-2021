package de.hpi.debs;

import de.tum.i13.bandency.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SyncSource implements SourceFunction<MeasurementOwn> {

    private static ChallengerGrpc.ChallengerBlockingStub challengeClient;
    private volatile boolean running = true;
    private long requested = 0;
    private final long batchNumbers;
    private final Benchmark benchmark;
    private static final long A_YEAR = Time.days(365).toMilliseconds();
    private static final String NOT_AVAILABLE = "-1";
    private final Locations locations;
    private LocationRetriever locationRetriever;
    private final HashMap<String, Long> cities;
    private final HashMap<String, Long> lastYearCities;
    private final HashMap<Tuple2<Float, Float>, String> locationsMap;

    public SyncSource(
            Benchmark benchmarkIn,
            long batchNumbersIn,
            Locations locations
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;

        this.locations = locations;
        this.cities = new HashMap<>();
        this.lastYearCities = new HashMap<>();
        this.locationsMap = new HashMap<>();
    }

    public SyncSource(Benchmark benchmarkIn, Locations locations) {

        benchmark = benchmarkIn;
        batchNumbers = Long.MAX_VALUE;

        this.locations = locations;
        this.cities = new HashMap<>();
        this.lastYearCities = new HashMap<>();
        this.locationsMap = new HashMap<>();
    }

    private String getCachedLocation(Measurement m) {
        return locationsMap.get(Tuple2.of(m.getLatitude(), m.getLongitude()));
    }

    private void addToCachedLocation(Measurement m, String city) {
        locationsMap.put(Tuple2.of(m.getLatitude(), m.getLongitude()), city);
    }

    public void processElement(Batch batch, SourceContext<MeasurementOwn> context) {
        // process the batch of events we have
        List<Measurement> currentYearList = batch.getCurrentList();
        List<Measurement> lastYearList = batch.getLastyearList();
        Measurement lastMeasurement;
        long watermarkTimestamp;
        if (currentYearList.isEmpty()) {
            lastMeasurement = lastYearList.get(lastYearList.size() - 1);
            watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000;
            watermarkTimestamp += lastMeasurement.getTimestamp().getNanos() / 1000;
            watermarkTimestamp += A_YEAR;
        } else {
            lastMeasurement = currentYearList.get(currentYearList.size() - 1);
            watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000;
            watermarkTimestamp += lastMeasurement.getTimestamp().getNanos() / 1000;
        }

        Optional<String> optionalCity;

        for (Measurement measurement : currentYearList) {
            String city = getCachedLocation(measurement);
            if (city == null) {
                optionalCity = locationRetriever.findCityForMeasurement(measurement);
                city = optionalCity.orElse(NOT_AVAILABLE);
                addToCachedLocation(measurement, city);
            }
            if (city.equals(NOT_AVAILABLE)) {
                continue;
            }
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, 0, city);

            Long lastTimestamp = cities.get(city);
            if (lastTimestamp == null) {
                cities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                cities.put(city, m.getTimestamp());
            }
            context.collectWithTimestamp(m, m.getTimestamp());
        }

        for (Measurement measurement : lastYearList) {
            String city = getCachedLocation(measurement);
            if (city == null) {
                optionalCity = locationRetriever.findCityForMeasurement(measurement);
                city = optionalCity.orElse(NOT_AVAILABLE);
                addToCachedLocation(measurement, city);
            }
            if (city.equals(NOT_AVAILABLE)) {
                continue;
            }
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, 0, city, A_YEAR, true);

            Long lastTimestamp = lastYearCities.get(city);
            if (lastTimestamp == null) {
                lastYearCities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                lastYearCities.put(city, m.getTimestamp());
            }
            context.collectWithTimestamp(m, m.getTimestamp());
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : cities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, false);
                context.collectWithTimestamp(watermark, watermark.getTimestamp());
            }
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : lastYearCities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, true);
                context.collectWithTimestamp(watermark, watermark.getTimestamp());
            }
        }
        context.emitWatermark(new Watermark(watermarkTimestamp));
    }

    @Override
    public void run(SourceContext<MeasurementOwn> context) throws IOException {
        this.locationRetriever = new LocationRetriever(locations);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        //for demo, we show the blocking stub
        challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        while (running) {
            Batch batch =  challengeClient.nextBatch(benchmark);
            processElement(batch, context);
            requested++;

            if (batch.getLast() || batchNumbers <= requested)
                running = false;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
