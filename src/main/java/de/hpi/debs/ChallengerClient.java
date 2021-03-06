package de.hpi.debs;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ChallengerClient {

    private static LocationRetriever locationRetriever;
    private final ChallengerGrpc.ChallengerStub asyncStub;
    private final ChallengerGrpc.ChallengerBlockingStub blockingStub;


    public ChallengerClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public ChallengerClient(ManagedChannelBuilder<?> channelBuilder) {
        ManagedChannel channel = channelBuilder
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();
        blockingStub = ChallengerGrpc.newBlockingStub(channel);
        asyncStub = ChallengerGrpc.newStub(channel);
    }

    public static void main(String[] args) throws Exception {
        ChallengerClient client = new ChallengerClient("challenge.msrg.in.tum.de", 5023);
        BenchmarkConfiguration configuration = BenchmarkConfiguration.newBuilder()
                .setToken(System.getenv("DEBS_API_KEY"))
                .setBatchSize(100)
                .setBenchmarkName("Testrun " + new Date().toString())
                .setBenchmarkType("test")
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .build();

        Benchmark benchmark = client.blockingStub.createNewBenchmark(configuration);

        Locations locations = getLocations(client, benchmark);
        locationRetriever = new LocationRetriever(locations);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        List<MeasurementOwn> measurements = new ArrayList<>();
        client.blockingStub.startBenchmark(benchmark);
        
        Batch batch;
        int count2 = 0;
        for (int i = 0; i < 20; i++) {
            batch = client.blockingStub.nextBatch(benchmark);
            count2 += batch.getCurrentCount();
            batch.getCurrentList().stream()
                    .map(MeasurementOwn::fromMeasurement)
                    .forEach(measurements::add);
        }
        DataStream<MeasurementOwn> measurementStream = env.fromCollection(measurements);
        AtomicInteger count = new AtomicInteger();
        measurementStream.map(m -> count.getAndIncrement());
        System.out.println(count2);

        DataStream<MeasurementOwn> cities = measurementStream.map(
                value -> {
                    value.setCity(locationRetriever.findCityForLocation(value.getPoint()));
                    return value;
                });

        WindowedStream<MeasurementOwn, String, TimeWindow> measurementByCity = cities
                .filter(m -> m.getCity().isPresent())
                .keyBy(m -> m.getCity().get())
                .window(SlidingEventTimeWindows.of(Time.hours(24), Time.minutes(5)));

        DiscardingSink<MeasurementOwn> sink = new DiscardingSink<>();
        cities.addSink(sink);

        client.blockingStub.endBenchmark(benchmark);
        env.execute("benchmark");
    }

    public static Locations getLocations(ChallengerClient client, Benchmark benchmark) {
        Locations locations;
        String locationFileName = "./locations.ser";
        if (new File(locationFileName).isFile()) {
            locations = readLocationsFromFile(locationFileName);
        } else {
            locations = client.blockingStub.getLocations(benchmark);
            saveLocationsToFile(locationFileName, locations);
        }
        return locations;
    }

    public static Locations readLocationsFromFile(String locationFileName) {
        Locations locations = null;
        try (
                FileInputStream streamIn = new FileInputStream(locationFileName);
                ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
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
                ObjectOutputStream oos = new ObjectOutputStream(fout);
        ) {
            oos.writeObject(locations);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
