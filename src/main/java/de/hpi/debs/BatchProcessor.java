package de.hpi.debs;

import de.tum.i13.bandency.Locations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Measurement;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BatchProcessor extends ProcessOperator<Batch, MeasurementOwn> {
    private static final long A_YEAR = Time.days(365).toMilliseconds();
    private static final String NOT_AVAILABLE = "-1";
    private final Locations locations;
    private LocationRetriever locationRetriever;
    private final HashMap<String, Long> cities;
    private final HashMap<String, Long> lastYearCities;
    private final HashMap<Tuple2<Float, Float>, String> locationsMap;

    public BatchProcessor(Locations locations) {
        super(new ProcessFunction<>() {
            @Override
            public void processElement(Batch value, Context ctx, Collector<MeasurementOwn> out) {
                // do nothing as we are doing everything in the operator
            }
        });

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

    @Override
    public void open() throws IOException {
        this.locationRetriever = new LocationRetriever(locations);
    }

    @Override
    public void processElement(StreamRecord<Batch> batch) {
        // process the batch of events we have
        List<Measurement> currentYearList = batch.getValue().getCurrentList();
        List<Measurement> lastYearList = batch.getValue().getLastyearList();
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
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city);

            Long lastTimestamp = cities.get(city);
            if (lastTimestamp == null) {
                cities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                cities.put(city, m.getTimestamp());
            }
            output.collect(new StreamRecord<>(m, m.getTimestamp()));

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
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, city, A_YEAR, true);

            Long lastTimestamp = lastYearCities.get(city);
            if (lastTimestamp == null) {
                lastYearCities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                lastYearCities.put(city, m.getTimestamp());
            }
            output.collect(new StreamRecord<>(m, m.getTimestamp()));

        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : cities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, false);
                output.collect(new StreamRecord<>(watermark, watermark.getTimestamp()));
            }
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : lastYearCities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                MeasurementOwn watermark = new MeasurementOwn(0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, true);
                output.collect(new StreamRecord<>(watermark, watermark.getTimestamp()));
            }
        }
        output.emitWatermark(new Watermark(watermarkTimestamp));
    }
}
