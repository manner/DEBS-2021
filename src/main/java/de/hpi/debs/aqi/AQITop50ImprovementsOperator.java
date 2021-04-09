package de.hpi.debs.aqi;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.stream.StreamSupport;

public class AQITop50ImprovementsOperator extends ProcessOperator<AQIImprovement, Void> {
    private static final int limit = 50;
    private final long benchmarkId;
    protected ListState<AQIImprovement> improvementsState;
    private int seqCounter;
    private ChallengerGrpc.ChallengerFutureStub challengeClient;
    private ManagedChannel channel;

    public AQITop50ImprovementsOperator(long benchmarkId) {
        super(new ProcessFunction<>() {
            @Override
            public void processElement(AQIImprovement value, Context ctx, Collector<Void> out) {
                // do nothing as we are doing everything in the operator
            }
        });
        this.benchmarkId = benchmarkId;
        this.seqCounter = 0;
    }

    @Override
    public void open() throws Exception {
        ListStateDescriptor<AQIImprovement> descriptor =
                new ListStateDescriptor<>(
                        "improvements",
                        AQIImprovement.class);

        improvementsState = getOperatorStateBackend().getListState(descriptor);

        channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        challengeClient = ChallengerGrpc.newFutureStub(channel)
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
    }

    @Override
    public void close() throws Exception {
        channel.shutdownNow();
    }

    @Override
    public void processElement(StreamRecord<AQIImprovement> value) throws Exception {
        improvementsState.add(value.getValue());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // Fix to avoid weird watermark in year 292278994
        if (mark.getTimestamp() > 1898553600000L) {
            return;
        }
        List<AQIImprovement> top50Improvements = StreamSupport.stream(improvementsState.get().spliterator(), false)
                .sorted(Comparator.comparingDouble(AQIImprovement::getImprovement).reversed())
                .limit(limit)
                .collect(Collectors.toList());

        List<TopKCities> topKCities = new ArrayList<>();
        for (int i = 0; i < top50Improvements.size(); i++) {
            AQIImprovement improvement = top50Improvements.get(i);
            TopKCities city = TopKCities.newBuilder()
                    .setPosition(i + 1)
                    .setCity(improvement.getCity())
                    .setAverageAQIImprovement(roundAndMultiply(improvement.getImprovement()))
                    .setCurrentAQIP1(roundAndMultiply(improvement.getCurAqiP1()))
                    .setCurrentAQIP2(roundAndMultiply(improvement.getCurAqiP2()))
                    .build();
            topKCities.add(city);
        }
//        topKCities.forEach(System.out::println);
        ResultQ1 result = ResultQ1.newBuilder()
                .addAllTopkimproved(topKCities)
                .setBatchSeqId(seqCounter++) // TODO: FIX THIS!
                .setBenchmarkId(benchmarkId)
                .build();

        challengeClient.resultQ1(result);
        improvementsState.clear();
    }

    private int roundAndMultiply(double value) {
        return (int) Math.round(value * 1000);
    }
}
