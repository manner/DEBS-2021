package de.hpi.debs.source;

import de.hpi.debs.LocationRetriever;
import de.hpi.debs.MeasurementOwn;
import de.tum.i13.bandency.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class SourceReaderOwn implements SourceReader<MeasurementOwn, SourceSplitOwn> {
    protected SourceReaderContext context;
    protected static ChallengerGrpc.ChallengerBlockingStub challengeClient;
    protected long requested = 0;
    protected final long batchNumbers;
    protected final Benchmark benchmark;
    protected List<SourceSplitOwn> snapshotState;
    protected boolean startup;
    protected boolean end;
    private static final long A_YEAR = Time.days(365).toMilliseconds();
    private static final String NOT_AVAILABLE = "-1";
    private final Locations locations;
    private LocationRetriever locationRetriever;
    private final HashMap<String, Long> cities;
    private final HashMap<String, Long> lastYearCities;
    private final HashMap<Tuple2<Float, Float>, String> locationsMap;

    public SourceReaderOwn(
            SourceReaderContext context,
            Benchmark benchmark,
            Locations locations,
            long batchNumbers
    ){
        this.context = context;
        this.benchmark = benchmark;
        this.batchNumbers = batchNumbers;
        this.locations = locations;
        this.snapshotState = null;
        this.end = false;
        this.startup = true;

        this.cities = new HashMap<>();
        this.lastYearCities = new HashMap<>();
        this.locationsMap = new HashMap<>();
    }

    @Override
    public void start() {
        try {
            this.locationRetriever = new LocationRetriever(locations);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        //for demo, we show the blocking stub
        challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
    }

    private String getCachedLocation(Measurement m) {
        return locationsMap.get(Tuple2.of(m.getLatitude(), m.getLongitude()));
    }

    private void addToCachedLocation(Measurement m, String city) {
        locationsMap.put(Tuple2.of(m.getLatitude(), m.getLongitude()), city);
    }

    public void processElement(Batch batch, ReaderOutput<MeasurementOwn> output) {
        // process the batch of events we have
        List<Measurement> currentYearList = batch.getCurrentList();
        List<Measurement> lastYearList = batch.getLastyearList();
        Measurement lastMeasurement;
        long watermarkTimestamp;
        if (currentYearList.isEmpty() && lastYearList.isEmpty())
            return;
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
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, batch.getSeqId(), city);

            Long lastTimestamp = cities.get(city);
            if (lastTimestamp == null) {
                cities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                cities.put(city, m.getTimestamp());
            }
            output.collect(m, m.getTimestamp());
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
            MeasurementOwn m = MeasurementOwn.fromMeasurement(measurement, batch.getSeqId(), city, A_YEAR, true);

            Long lastTimestamp = lastYearCities.get(city);
            if (lastTimestamp == null) {
                lastYearCities.putIfAbsent(city, m.getTimestamp());
            } else if (m.getTimestamp() > lastTimestamp) {
                lastYearCities.put(city, m.getTimestamp());
            }
            output.collect(m, m.getTimestamp());
        }

        boolean notFirst = false;
        MeasurementOwn watermark = null;

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : cities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                if (notFirst) {
                    output.collect(watermark, watermark.getTimestamp());
                } else {
                    notFirst = true;
                }
                watermark = new MeasurementOwn(batch.getSeqId(), 0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, false);
            }
        }

        // send watermarks for each city in batch
        for (Map.Entry<String, Long> city : lastYearCities.entrySet()) {
            long lastTimestampOfCity = city.getValue();

            // check if city is active
            if (lastTimestampOfCity >= watermarkTimestamp - Time.minutes(10).toMilliseconds()) {
                if (notFirst) {
                    output.collect(watermark, watermark.getTimestamp());
                } else {
                    notFirst = true;
                }
                watermark = new MeasurementOwn(batch.getSeqId(), 0, 0, 0, 0, watermarkTimestamp, city.getKey(), true, true);
            }
        }

        // emit last watermark
        if (notFirst) {
            watermark.setLastWatermarkOfBatch();
            output.collect(watermark, watermark.getTimestamp());
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<MeasurementOwn> output) {
        Batch batch = null;

        if (startup) {
            challengeClient.startBenchmark(benchmark);
            startup = false;
        }

        if (!end) {
            batch = challengeClient.nextBatch(benchmark);
            processElement(batch, output);
            requested++;
        }

        if (end || batch.getLast() || batchNumbers <= requested) {
            end = false;
            return InputStatus.END_OF_INPUT;
        }
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<SourceSplitOwn> snapshotState(long checkpointId) {
        if (snapshotState != null)
            return snapshotState;
        return new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return new CompletableFuture<>();
    }

    @Override
    public void addSplits(List<SourceSplitOwn> splits) {
        if (snapshotState != null)
            snapshotState.addAll(splits);

        snapshotState = new ArrayList<>(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
    }
}
