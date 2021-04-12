package de.hpi.debs.aqi;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.ResultQ1;
import de.tum.i13.bandency.TopKCities;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class AQITop50ImprovementsOperator extends ProcessOperator<AQIImprovement, Void> {
    private static final int limit = 50;
    private final long benchmarkId;
    protected List<AQIImprovement> improvementsState;
    private long lastWatermark;
    private int seqCounter;
    private ChallengerGrpc.ChallengerFutureStub challengeClient;
    private ManagedChannel channel;
    private TopKCities.Builder topKCitiesBuilder;
    private ResultQ1.Builder resultBuilder;

    public AQITop50ImprovementsOperator(long benchmarkId) {
        super(new ProcessFunction<>() {
            @Override
            public void processElement(AQIImprovement value, Context ctx, Collector<Void> out) {
                // do nothing as we are doing everything in the operator
            }
        });
        this.benchmarkId = benchmarkId;
        this.seqCounter = 0;
        this.lastWatermark = Long.MIN_VALUE;
        this.improvementsState = new ArrayList<>();
    }

    @Override
    public void open() throws Exception {
        channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        challengeClient = ChallengerGrpc.newFutureStub(channel)
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        topKCitiesBuilder = TopKCities.newBuilder();
        resultBuilder = ResultQ1.newBuilder();
    }

    @Override
    public void close() throws Exception {
        channel.shutdownNow();
    }

    @Override
    public void processElement(StreamRecord<AQIImprovement> value) throws Exception {
        AQIImprovement improvement = value.getValue();

        // late events should be discarded
        if (improvement.getTimestamp() < lastWatermark) {
            return;
        }

        // window for improvement didn't have any events in it
        if (improvement.isWatermark() && improvement.getImprovement() < 0) {
            return;
        }

        improvementsState.add(improvement);
    }

    @Override
    public void processWatermark(Watermark mark) {
        // Fix to avoid weird watermark in year 292278994
        if (mark.getTimestamp() > 1898553600000L) {
            return;
        }
        lastWatermark = mark.getTimestamp();

        List<AQIImprovement> top50Improvements = improvementsState.stream()
                .filter(aqiImprovement -> aqiImprovement.getSeq() == seqCounter)
                .sorted(Comparator.comparingDouble(AQIImprovement::getImprovement).reversed())
                .limit(limit)
                .collect(Collectors.toList());

        improvementsState = improvementsState.stream()
                        .filter(aqiImprovement -> aqiImprovement.getSeq() > seqCounter)
                        .collect(Collectors.toList());

        List<TopKCities> topKCities = new ArrayList<>();
        for (int i = 0; i < top50Improvements.size(); i++) {
            AQIImprovement improvement = top50Improvements.get(i);
            topKCitiesBuilder.clear();
            TopKCities city = topKCitiesBuilder
                    .setPosition(i + 1)
                    .setCity(improvement.getCity())
                    .setAverageAQIImprovement(roundAndMultiply(improvement.getImprovement()))
                    .setCurrentAQIP1(roundAndMultiply(improvement.getCurAqiP1()))
                    .setCurrentAQIP2(roundAndMultiply(improvement.getCurAqiP2()))
                    .build();
            topKCities.add(city);
        }

        resultBuilder.clear();
        ResultQ1 result = resultBuilder
                .addAllTopkimproved(topKCities)
                .setBatchSeqId(seqCounter++)
                .setBenchmarkId(benchmarkId)
                .build();

        challengeClient.resultQ1(result);
    }

    private int roundAndMultiply(double value) {
        return (int) Math.round(value * 1000);
    }
}
