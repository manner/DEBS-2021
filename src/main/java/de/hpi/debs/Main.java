package de.hpi.debs;

import de.hpi.debs.aqi.*;
import de.hpi.debs.serializer.LocationSerializer;
import de.tum.i13.bandency.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.twitter.chill.protobuf.ProtobufSerializer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {

        long currentStart = LocalDateTime.of(2020, Month.JANUARY, 1, 0, 0).getNano() / 1000;

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        ChallengerGrpc.ChallengerBlockingStub challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        int BATCH_SIZE = Integer.parseInt(System.getenv("BATCH_SIZE"));
        String BENCHMARK_TYPE = System.getenv("BENCHMARK_TYPE");
        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName(System.getenv("BENCHMARK_NAME_PREFIX") + new Date())
                .setBatchSize(BATCH_SIZE)
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .addQueries(BenchmarkConfiguration.Query.Q2)
                .setToken(System.getenv("DEBS_API_KEY")) // go to: https://challenge.msrg.in.tum.de/profile/
                .setBenchmarkType(BENCHMARK_TYPE) // Benchmark Type for testing
                .build();

        //long CHECKPOINTING_INTERVAL = Long.parseLong(System.getenv("CHECKPOINTING_INTERVAL"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int PARALLELISM = Integer.parseInt(System.getenv("PARALLELISM"));
        env.setParallelism(PARALLELISM); // sets the number of parallel for each instance
        //env.enableCheckpointing(CHECKPOINTING_INTERVAL);

        // Create a new Benchmark
        Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);
        Locations locations = LocationSerializer.getLocations(challengeClient, newBenchmark);
        LocationRetriever locationRetriever = new LocationRetriever(locations);

//        DataStream<MeasurementOwn> cities = env.addSource(new StreamGenerator(newBenchmark, 3));

        System.out.println(challengeClient.startBenchmark(newBenchmark));

        env.getConfig().registerTypeWithKryoSerializer(Batch.class, ProtobufSerializer.class);

        DataStream<Batch> batches = AsyncDataStream.orderedWait(
                env.fromSequence(0, 5),
                new AsyncStreamGenerator(newBenchmark),
                1000,
                TimeUnit.SECONDS,
                4);
        batches.print();
        DiscardingSink<Batch> sink =  new DiscardingSink<>();
        batches.addSink(sink);

//        DataStream<MeasurementOwn> cities = batches
//                .transform(
//                        "batchProcessor",
//                        TypeInformation.of(MeasurementOwn.class),
//                        new BatchProcessor(locationRetriever)
//                ).setParallelism(1);
//        cities.print();
//        env.fromSequence(1, 10)
//                AsyncDat
//                        (new AsyncStreamGenerator(newBenchmark));

//        DataStream<MeasurementOwn> lastYearCities = cities.filter(MeasurementOwn::isLastYear);
//        DataStream<MeasurementOwn> currentYearCities = cities.filter(MeasurementOwn::isCurrentYear);
//
//        DataStream<AQIValue24h> aqiStreamCurrentYear = currentYearCities
//                .keyBy(MeasurementOwn::getCity)
//                .transform(
//                        "AQIValue24hProcessOperator",
//                        TypeInformation.of(AQIValue24h.class),
//                        new AQIValue24hProcessOperator(currentStart)
//                );
//
//        DataStream<AQIValue24h> aqiStreamLastYear = lastYearCities
//                .keyBy(MeasurementOwn::getCity)
//                .transform(
//                        "AQIValue24hProcessOperator",
//                        TypeInformation.of(AQIValue24h.class),
//                        new AQIValue24hProcessOperator(currentStart)
//                );
//
//        DataStream<AQIValue5d> fiveDayStreamCurrentYear = aqiStreamCurrentYear // need more attributes
//                .keyBy(AQIValue24h::getCity)
//                .transform(
//                        "AQIValue5dProcessOperator",
//                        TypeInformation.of(AQIValue5d.class),
//                        new AQIValue5dProcessOperator(currentStart, false)
//                );
//
//        DataStream<AQIValue5d> fiveDayStreamLastYear = aqiStreamLastYear // need more attributes
//                .keyBy(AQIValue24h::getCity)
//                .transform(
//                        "AQIValue5dProcessOperator",
//                        TypeInformation.of(AQIValue5d.class),
//                        new AQIValue5dProcessOperator(currentStart, true)
//                );
//
//        DataStream<AQIImprovement> fiveDayImprovement = fiveDayStreamCurrentYear
//                .keyBy(AQIValue5d::getCity)
//                .intervalJoin(fiveDayStreamLastYear.keyBy(AQIValue5d::getCity))
//                .between(Time.milliseconds(0), Time.milliseconds(0))
//                .process(new AQIImprovementProcessor());
//
//        fiveDayImprovement
//                .transform(
//                        "top50cities",
//                        TypeInformation.of(Void.class),
//                        new AQITop50ImprovementsOperator(newBenchmark.getId())
//                ).setParallelism(1);
//
//
//        aqiStreamCurrentYear
//                .keyBy(AQIValue24h::getCity)
//                .process(new LongestStreakProcessor())
//                .transform(
//                        "histogram",
//                        TypeInformation.of(Void.class),
//                        new HistogramOperator(newBenchmark.getId())
//                ).setParallelism(1);

        //Start the benchmark
        //System.out.println(chal   1lengeClient.startBenchmark(newBenchmark));
        System.out.println("started Benchmark");
        env.execute("benchmark");
        //System.out.println(challengeClient.endBenchmark(newBenchmark));
        System.out.println("ended Benchmark");

        channel.shutdown();

        System.exit(0);
    }
}

