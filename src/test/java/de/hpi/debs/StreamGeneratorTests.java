package de.hpi.debs;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.google.protobuf.Timestamp;
import de.hpi.debs.serializer.LocationSerializer;
import de.hpi.debs.testHarness.SourceFunctionMocker;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;
import de.tum.i13.bandency.Measurement;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamGeneratorTests {

    public static class StreamGeneratorTestClass extends StreamGenerator {

        public StreamGeneratorTestClass(
                int batchNumbersIn) {
            super(null, batchNumbersIn);
        }
    }

    public static class CreateBatch {

        Batch.Builder bBuilder;
        Measurement.Builder mBuilder;
        Timestamp.Builder tBuilder;

        public CreateBatch() {
            this.bBuilder = Batch.newBuilder();
            this.mBuilder = Measurement.newBuilder();
            this.tBuilder = Timestamp.newBuilder();
        }

        public CreateBatch clear() {
            bBuilder.clear();

            return this;
        }

        public CreateBatch addCurrent(float p1, float p2, float latitude, float longitude, long seconds, int nanos) {
            this.tBuilder.clear();
            this.tBuilder.setSeconds(seconds);
            this.tBuilder.setNanos(nanos);

            this.mBuilder.clear();
            this.mBuilder.setP1(p1);
            this.mBuilder.setP2(p2);
            this.mBuilder.setLatitude(latitude);
            this.mBuilder.setLongitude(longitude);
            this.mBuilder.setTimestamp(this.tBuilder.buildPartial());

            this.bBuilder.addCurrent(this.mBuilder.buildPartial());

            return this;
        }

        public CreateBatch addLastYear(float p1, float p2, float latitude, float longitude, long seconds, int nanos) {
            this.tBuilder.setSeconds(seconds);
            this.tBuilder.setNanos(nanos);

            this.mBuilder.setP1(p1);
            this.mBuilder.setP2(p2);
            this.mBuilder.setLatitude(latitude);
            this.mBuilder.setLongitude(longitude);
            this.mBuilder.setTimestamp(this.tBuilder.buildPartial());

            this.bBuilder.addLastyear(this.mBuilder.buildPartial());

            return this;
        }

        public Batch build(boolean last) {
            this.bBuilder.setLast(last);

            return this.bBuilder.buildPartial();
        }
    }

    CreateBatch generator = new CreateBatch();

    ArrayList<Batch> batches = new ArrayList<>() {{
        add(generator.clear()
                .addCurrent(1.0F, 88.0F, 52.532F, 13.328F, 31536030L, 100) // Berlin Moabit
                .addCurrent(15.0F, 2.0F, 51.42F, 6.971F, 31536040L, 1000) // Essen
                .addCurrent(100.0F, 2.0F, 0.0F, 0.0F, 31536031L, 1020) // Hopefully will be filtered out
                .addLastYear(1.0F, 2.1F, 52.532F, 13.328F, 10L, 100) // Berlin Moabit
                .addLastYear(1.1F, 2.0F, 51.42F, 6.971F, 13L, 100) // Essen
                .addLastYear(123.0F, 2.0F, 52.532F, 13.328F, 100L, 1050) // Berlin Moabit
                .addLastYear(1.5F, 2.0F, 52.532F, 13.328F, 340L, 100) // Berlin Moabit
                .addCurrent(1.0F, 2.5F, 52.532F, 13.328F, 31536030L, 100) // Berlin Moabit
                .addCurrent(1.0F, 200.0F, 52.532F, 13.328F, 31536030L, 100) // Berlin Moabit
                .addLastYear(1.0F, 211.0F, 51.42F, 6.971F, 60L, 100) // Essen
                .addLastYear(1.0F, 2.2F, 51.42F, 6.971F, 40L, 100) // Essen
                .addLastYear(10.0F, 2.0F, 51.42F, 6.971F, 80L, 1090) // Essen
                .addCurrent(10.0F, 2.0F, 51.42F, 6.971F, 31536040L, 1000) // Essen
                .addCurrent(1.0F, 20.0F, 52.532F, 13.328F, 31536030L, 1400) // Berlin Moabit
                .addLastYear(14.0F, 221.0F, 52.532F, 13.328F, 130L, 100) // Berlin Moabit
                .addLastYear(3.0F, 2.0F, 51.42F, 6.971F, 90L, 1000) // Essen
                .addCurrent(1.0F, 3.0F, 51.42F, 6.971F, 31536340L, 13000) // Essen
                .build(false)
        );
        add(generator.clear()
                .addCurrent(4.0F, 2.0F, 52.532F, 13.328F, 31536030L, 100) // Berlin Moabit
                .addCurrent(1.0F, 2.0F, 51.42F, 6.971F, 31536040L, 1000) // Essen
                .addCurrent(100.0F, 2.0F, 0.0F, 0.0F, 31536431L, 1020) // Hopefully will be filtered out
                .addLastYear(1.0F, 2.0F, 52.532F, 13.328F, 100L, 100) // Berlin Moabit
                .addLastYear(1.0F, 2.0F, 51.42F, 6.971F, 13L, 100) // Essen
                .addCurrent(1.0F, 20.0F, 52.532F, 13.328F, 31536030L, 1400) // Berlin Moabit
                .addLastYear(123.0F, 2.0F, 52.532F, 13.328F, 100L, 1050) // Berlin Moabit
                .addLastYear(1.0F, 2.0F, 52.532F, 13.328F, 340L, 100) // Berlin Moabit
                .addCurrent(1.0F, 2.0F, 52.532F, 13.328F, 31537035L, 100) // Berlin Moabit
                .addCurrent(1.0F, 200.0F, 52.532F, 13.328F, 31537030L, 100) // Berlin Moabit
                .addLastYear(1.0F, 24.0F, 51.42F, 6.971F, 7060L, 100) // Essen
                .addLastYear(1.8F, 2.0F, 51.42F, 6.971F, 7040L, 100) // Essen
                .addLastYear(10.0F, 2.0F, 51.42F, 6.971F, 7080L, 1090) // Essen
                .addCurrent(10.0F, 2.0F, 51.42F, 6.971F, 31537140L, 1000) // Essen
                .addCurrent(1.0F, 20.0F, 52.532F, 13.328F, 31537030L, 1400) // Berlin Moabit
                .addLastYear(14.0F, 221.0F, 52.532F, 13.328F, 7430L, 100) // Berlin Moabit
                .addLastYear(17.0F, 2.0F, 51.42F, 6.971F, 7090L, 1000) // Essen
                .addCurrent(1.0F, 2.0F, 51.42F, 6.971F, 31537340L, 13000) // Essen
                .build(false)
        );
        add(generator.clear()
                .addCurrent(1.0F, 2.0F, 52.532F, 13.328F, 31539030L, 100) // Berlin Moabit
                .addCurrent(1.0F, 2.0F, 51.42F, 6.971F, 31539040L, 1000) // Essen
                .addCurrent(100.0F, 2.0F, 0.0F, 0.0F, 31539031L, 1020) // Hopefully will be filtered out
                .addLastYear(1.0F, 2.0F, 52.532F, 13.328F, 9010L, 100) // Berlin Moabit
                .addLastYear(1.0F, 2.0F, 51.42F, 6.971F, 9013L, 100) // Essen
                .addLastYear(1.0F, 20.0F, 52.532F, 13.328F, 30L, 1400) // Berlin Moabit
                .addLastYear(123.0F, 2.0F, 52.532F, 13.328F, 9100L, 1050) // Berlin Moabit
                .addLastYear(1.0F, 24.0F, 52.532F, 13.328F, 9340L, 100) // Berlin Moabit
                .addCurrent(1.0F, 2.0F, 52.532F, 13.328F, 31539230L, 100) // Berlin Moabit
                .addCurrent(1.0F, 200.0F, 52.532F, 13.328F, 31536030L, 100) // Berlin Moabit
                .addLastYear(14.0F, 211.0F, 51.42F, 6.971F, 9060L, 100) // Essen
                .addLastYear(1.0F, 2.0F, 51.42F, 6.971F, 40L, 100) // Essen
                .addLastYear(10.0F, 2.0F, 51.42F, 6.971F, 9094L, 1090) // Essen
                .addCurrent(10.0F, 2.0F, 51.42F, 6.971F, 31539070L, 1000) // Essen
                .addCurrent(1.0F, 260.0F, 52.532F, 13.328F, 31536030L, 1400) // Berlin Moabit
                .addLastYear(14.0F, 221.0F, 52.532F, 13.328F, 90180L, 100) // Berlin Moabit
                .addLastYear(1.0F, 2.0F, 51.42F, 6.971F, 9090L, 1000) // Essen
                .addCurrent(1.0F, 2.0F, 51.42F, 6.971F, 31539340L, 13000) // Essen
                .build(true)
        );
    }};

    @Test
    public void generalTest() throws IOException {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();
        Main.challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName("Testrun " + new Date().toString())
                .setBatchSize(1000)
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .addQueries(BenchmarkConfiguration.Query.Q2)
                .setToken(System.getenv("DEBS_API_KEY")) // go to: https://challenge.msrg.in.tum.de/profile/
                .setBenchmarkType("test") // Benchmark Type for testing
                .build();
        Benchmark newBenchmark = Main.challengeClient.createNewBenchmark(bc);
        Locations locations = LocationSerializer.getLocations(Main.challengeClient, newBenchmark);
        Main.locationRetriever = new LocationRetriever(locations);

        StreamGeneratorTestClass source = new StreamGeneratorTestClass(0);
        SourceFunctionMocker<MeasurementOwn> testContext = new SourceFunctionMocker<>();
        ArrayList<StreamRecord<MeasurementOwn>> groundTruth = new ArrayList<>();
        long timestamp;
        MeasurementOwn berlin = null;
        MeasurementOwn essen = null;

        try {
            for (Batch batch : batches) {
                List<Measurement> allMeasurements = new ArrayList<>();
                allMeasurements.addAll(batch.getCurrentList());
                allMeasurements.addAll(batch.getLastyearList());
                for (Measurement m : allMeasurements) { // events should be emitted in same order as they are in batch
                    if (m.getLatitude() != 0.0F) {
                        timestamp = (long) (m.getTimestamp().getSeconds() * 1000.0 + m.getTimestamp().getNanos() / 1000.0);
                        if (m.getLatitude() == 51.42F) { // Essen
                            essen = new MeasurementOwn(
                                    m.getP1(),
                                    m.getP2(),
                                    m.getLatitude(),
                                    m.getLongitude(),
                                    timestamp,
                                    "Essen",
                                    false);

                            groundTruth.add(new StreamRecord<>(essen, timestamp));
                        } else { // "Berlin Moabit"
                            berlin = new MeasurementOwn(
                                    m.getP1(),
                                    m.getP2(),
                                    m.getLatitude(),
                                    m.getLongitude(),
                                    timestamp,
                                    "Berlin Moabit",
                                    false);

                            groundTruth.add(new StreamRecord<>(berlin, timestamp));
                        }
                    }
                }
                Measurement lastMeasurement = batch.getCurrentList().get(batch.getCurrentList().size() - 1);
                long watermarkTimestamp = lastMeasurement.getTimestamp().getSeconds() * 1000 + lastMeasurement.getTimestamp().getNanos() / 1000;
                if (berlin != null) {
                    if (watermarkTimestamp < 31536000000L) {
                        berlin = MeasurementOwn.createWatermark(watermarkTimestamp, "Berlin Moabit");
                        groundTruth.add(new StreamRecord<>(berlin, berlin.getTimestamp()));
                        berlin = MeasurementOwn.createWatermark(watermarkTimestamp + 31536000000L, "Berlin Moabit");
                        groundTruth.add(new StreamRecord<>(berlin, berlin.getTimestamp()));
                    } else {
                        berlin = MeasurementOwn.createWatermark(watermarkTimestamp, "Berlin Moabit");
                        groundTruth.add(new StreamRecord<>(berlin, berlin.getTimestamp()));
                        berlin = MeasurementOwn.createWatermark(watermarkTimestamp - 31536000000L, "Berlin Moabit");
                        groundTruth.add(new StreamRecord<>(berlin, berlin.getTimestamp()));
                    }
                }
                if (essen != null) {
                    if (watermarkTimestamp < 31536000000L) {
                        essen = MeasurementOwn.createWatermark(watermarkTimestamp, "Essen");
                        groundTruth.add(new StreamRecord<>(essen, essen.getTimestamp()));
                        essen = MeasurementOwn.createWatermark(watermarkTimestamp + 31536000000L, "Essen");
                        groundTruth.add(new StreamRecord<>(essen, essen.getTimestamp()));
                    } else {
                        essen = MeasurementOwn.createWatermark(watermarkTimestamp, "Essen");
                        groundTruth.add(new StreamRecord<>(essen, essen.getTimestamp()));
                        essen = MeasurementOwn.createWatermark(watermarkTimestamp - 31536000000L, "Essen");
                        groundTruth.add(new StreamRecord<>(essen, essen.getTimestamp()));
                    }
                }
                source.processBatch(testContext, batch);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        int i = 0;

        // check if every event is in ground truth
        for (Object item : testContext.getOutput()) {
            if (item.getClass() == StreamRecord.class) {
                assertTrue(groundTruth.contains(item), "missing event: " + item);
                i++;
            }
        }

        assertEquals(i, groundTruth.size()); // check if we have the same number of events emitted as ground truth
    }
}
