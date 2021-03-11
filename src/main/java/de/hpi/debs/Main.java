package de.hpi.debs;

import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import de.hpi.debs.aqi.AQIValue;
import de.hpi.debs.aqi.AQIValueProcessor;
import de.hpi.debs.aqi.AverageAQIAggregate;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;

public class Main extends Base {

    public static ChallengerGrpc.ChallengerBlockingStub challengeClient;
    private static LocationRetriever locationRetriever;

    public static void main(String[] args) throws Exception {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();


        challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
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

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism); // sets the number of parallel for each instance

        // Create a new Benchmark
        Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

        // Get the locations
        Locations locations = getLocations(challengeClient, newBenchmark);
        locationRetriever = new LocationRetriever(locations);
        //System.out.println(locations);

        DataStream<MeasurementOwn> measurementStream = env.addSource(
                measurementsSourceOrTest(new StreamGenerator(newBenchmark, 3))
        );

        DataStream<MeasurementOwn> cities = measurementStream.map(
                value -> {
                    value.setCity(locationRetriever.findCityForLocation(value.getPoint()));
                    return value;
                });

        DataStream<AQIValue> aqiStream = cities
                .filter(m -> m.getCity().isPresent())
                .keyBy(m -> m.getCity().get())
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(5)))
                .aggregate(new AverageAQIAggregate(), new AQIValueProcessor());

        DiscardingSink<AQIValue> sink = new DiscardingSink<>();
        aqiStream.addSink(sink);

        //Start the benchmark
        System.out.println(challengeClient.startBenchmark(newBenchmark));
        env.execute("benchmark");
        System.out.println(challengeClient.endBenchmark(newBenchmark));
        System.out.println("ended Benchmark");
    }

    public static Locations getLocations(ChallengerGrpc.ChallengerBlockingStub client, Benchmark benchmark) {
        Locations locations;
        String locationFileName = "./locations.ser";
        if (new File(locationFileName).isFile()) {
            locations = readLocationsFromFile(locationFileName);
        } else {
            locations = client.getLocations(benchmark);
            saveLocationsToFile(locationFileName, locations);
        }
        return locations;
    }

    public static Locations readLocationsFromFile(String locationFileName) {
        Locations locations = null;
        try (
                FileInputStream streamIn = new FileInputStream(locationFileName);
                ObjectInputStream objectinputstream = new ObjectInputStream(streamIn)
        ) {
            locations = (Locations) objectinputstream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return locations;
    }

    private static void saveLocationsToFile(String locationFileName, Locations locations) {
        try (
                FileOutputStream fout = new FileOutputStream(locationFileName, true);
                ObjectOutputStream oos = new ObjectOutputStream(fout)
        ) {
            oos.writeObject(locations);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
