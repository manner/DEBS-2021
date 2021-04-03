package de.hpi.debs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import de.hpi.debs.aqi.AQIImprovement;
import de.hpi.debs.aqi.AQIImprovementProcessor;
import de.hpi.debs.aqi.AQITop50Improvements;
import de.hpi.debs.aqi.AQIValue24h;
import de.hpi.debs.aqi.AQIValue24hProcessOperator;
import de.hpi.debs.aqi.AQIValue5d;
import de.hpi.debs.aqi.AQIValue5dProcessOperator;
import de.hpi.debs.aqi.LongestStreakProcessor;
import de.hpi.debs.serializer.LocationSerializer;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Date;

public class Main {

    public static ChallengerGrpc.ChallengerBlockingStub challengeClient;
    public static LocationRetriever locationRetriever;

    public static void main(String[] args) throws Exception {

        long currentStart = 1577833200000L;
        long lastStart = currentStart - Time.days(365).toMilliseconds(); // 365 days before

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
        env.setParallelism(4); // sets the number of parallel for each instance

        // Create a new Benchmark
        Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

        // Get the locations
        Locations locations = LocationSerializer.getLocations(challengeClient, newBenchmark);
        locationRetriever = new LocationRetriever(locations);
        //System.out.println(locations);

        DataStream<MeasurementOwn> cities = env.addSource(new StreamGenerator(newBenchmark, 3));

        DataStream<MeasurementOwn> lastYearCities = cities.filter(MeasurementOwn::isLastYear);
        DataStream<MeasurementOwn> currentYearCities = cities.filter(MeasurementOwn::isCurrentYear);

        DataStream<AQIValue24h> aqiStreamCurrentYear = currentYearCities
                .keyBy(MeasurementOwn::getCity)
                .transform(
                        "AQIValue24hProcessOperator",
                        TypeInformation.of(AQIValue24h.class),
                        new AQIValue24hProcessOperator(currentStart)
                );

        DataStream<AQIValue24h> aqiStreamLastYear = lastYearCities
                .keyBy(MeasurementOwn::getCity)
                .transform(
                        "AQIValue24hProcessOperator",
                        TypeInformation.of(AQIValue24h.class),
                        new AQIValue24hProcessOperator(lastStart)
                );

        DataStream<AQIValue5d> fiveDayStreamCurrentYear = aqiStreamCurrentYear // need more attributes
                .keyBy(AQIValue24h::getCity)
                .transform(
                        "AQIValue5dProcessOperator",
                        TypeInformation.of(AQIValue5d.class),
                        new AQIValue5dProcessOperator(currentStart)
                );

        DataStream<AQIValue5d> fiveDayStreamLastYear = aqiStreamLastYear // need more attributes
                .keyBy(AQIValue24h::getCity)
                .transform(
                        "AQIValue5dProcessOperator",
                        TypeInformation.of(AQIValue5d.class),
                        new AQIValue5dProcessOperator(lastStart)
                );

        DataStream<AQIImprovement> fiveDayImprovement = fiveDayStreamCurrentYear
                .keyBy(AQIValue5d::getCity)
                .intervalJoin(fiveDayStreamLastYear.keyBy(AQIValue5d::getCity))
                .between(Time.days(-365), Time.days(-365))
                .process(new AQIImprovementProcessor());

        DataStream<AQIImprovement> top50 = fiveDayImprovement
                .keyBy(AQIImprovement::getTimestamp)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new AQITop50Improvements());

        top50.print();

        aqiStreamCurrentYear
                .keyBy(AQIValue24h::getCity)
                .process(new LongestStreakProcessor())
                .transform(
                        "histogram",
                        TypeInformation.of(Void.class),
                        new HistogramOperator(newBenchmark.getId())
                );

        //seven day window is little bit different than the five day window and will not use the "rolling" processor

        DiscardingSink<AQIImprovement> sink = new DiscardingSink<>();
        top50.addSink(sink);

        //Start the benchmark
        System.out.println(challengeClient.startBenchmark(newBenchmark));
        env.execute("benchmark");
        System.out.println(challengeClient.endBenchmark(newBenchmark));
        System.out.println("ended Benchmark");
    }


}

