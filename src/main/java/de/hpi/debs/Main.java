package de.hpi.debs;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Main {

    private static LocationRetriever locationRetriever;

    public static void main(String[] args)  throws Exception { //we should handle the exception in code

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();


        var challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName("Testrun " + new Date().toString())
                .setBatchSize(1000)
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .addQueries(BenchmarkConfiguration.Query.Q2)
                .setToken("kfhlzrortvxxgywlghvtmmohhagkfzkv") //go to: https://challenge.msrg.in.tum.de/profile/
                .setBenchmarkType("test") //Benchmark Type for testing
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<MeasurementOwn> measurements = new ArrayList<>();

        //Create a new Benchmark
        Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

        //Get the locations
        Locations locations = getLocations(challengeClient, newBenchmark);
        locationRetriever = new LocationRetriever(locations);
        System.out.println(locations);


        //Start the benchmark
        System.out.println(challengeClient.startBenchmark(newBenchmark));

        //Process the events
        int cnt = 0;
        while(true) {
            Batch batch = challengeClient.nextBatch(newBenchmark);
            if (batch.getLast()) { //Stop when we get the last batch
                System.out.println("Received last batch, finished!");
                break;
            }

            //process the batch of events we have
            batch.getCurrentList().stream()
                    .map(MeasurementOwn::fromMeasurement)
                    .forEach(measurements::add);
            MeasurementOwn m = measurements.get(batch.getCurrentCount() - 1);
            m.setWatermark(true);
            measurements.set(batch.getCurrentCount() - 1, m);

            System.out.println("Processed batch #" + cnt);
            ++cnt;

            if(cnt > 2) { //for testing you can
                break;
            }
        }

        WatermarkStrategy<MeasurementOwn> watermarkStrategy = WatermarkStrategy
                .forGenerator((context) -> new MeasurementWatermarkGenerator())
                .withTimestampAssigner(((element, timestamp) -> element.getTimestamp()));

        DataStream<MeasurementOwn> measurementStream = env.fromCollection(measurements)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        DataStream<MeasurementOwn> cities = measurementStream.map(
                value -> {
                    value.setCity(locationRetriever.findCityForLocation(value.getPoint()));
                    return value;
                });

        WindowedStream<MeasurementOwn, String, TimeWindow> measurementByCity = cities
                .filter(m -> m.getCity().isPresent())
                .keyBy(m -> m.getCity().get())
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.minutes(5)));

        DataStream<Integer> aqiStream = measurementByCity
                .aggregate(new AverageAQIAggregate());

        aqiStream.print();
        DiscardingSink<MeasurementOwn> sink = new DiscardingSink<>();
        cities.addSink(sink);

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
