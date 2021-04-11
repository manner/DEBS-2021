package de.hpi.debs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import de.hpi.debs.aqi.Streak;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.ResultQ2;
import de.tum.i13.bandency.TopKStreaks;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistogramOperator extends ProcessOperator<Streak, Void> {

    private final long benchmarkId;
    private long seq;
    protected ListState<Streak> streaks;
    private int seqCounter;
    private ChallengerGrpc.ChallengerFutureStub challengeClient;
    private ManagedChannel channel;
    private TopKStreaks.Builder topKStreaksBuilder;
    private ResultQ2.Builder resultBuilder;

    public HistogramOperator(long benchmarkId) {
        super(new ProcessFunction<>() {
            @Override
            public void processElement(Streak value, Context ctx, Collector<Void> out) {
                // do nothing as we are doing everything in the operator
            }
        });
        this.benchmarkId = benchmarkId;
        this.seqCounter = 0;
    }

    @Override
    public void open() throws Exception {
        ListStateDescriptor<Streak> descriptor =
                new ListStateDescriptor<>(
                        "streaks",
                        Streak.class);

        streaks = getOperatorStateBackend().getListState(descriptor);
        channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        challengeClient = ChallengerGrpc.newFutureStub(channel)
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        topKStreaksBuilder = TopKStreaks.newBuilder();
        resultBuilder = ResultQ2.newBuilder();
    }

    @Override
    public void close() throws Exception {
        channel.shutdownNow();
    }

    @Override
    public void processElement(StreamRecord<Streak> value) throws Exception {
        // TODO: Handle early elements and store them and discard late events
        streaks.add(value.getValue());
        this.seq = value.getValue().getSeq();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // Fix to avoid weird watermark in year 292278994
        if (mark.getTimestamp() > 1898553600000L) {
            return;
        }
        List<TopKStreaks> topKStreaks = calculate(streaks.get(), mark.getTimestamp());
        resultBuilder.clear();
        ResultQ2 result = resultBuilder
                .addAllHistogram(topKStreaks)
                .setBatchSeqId(seq)//seqCounter++) // TODO: FIX THIS!
                .setBenchmarkId(benchmarkId)
                .build();

        challengeClient.resultQ2(result);
        streaks.clear();
    }

    private int getBucketSize(long watermarkTimestamp) {
        long firstTimestampInBatch = 1583020800000L; // TODO: FIX THIS!
        long bucketSize = Math.max(0, (watermarkTimestamp - firstTimestampInBatch) / 14);
        long maxBucketSize = Time.days(7).toMilliseconds() / 14;
        return (int) Math.min(bucketSize, maxBucketSize);
    }

    private List<TopKStreaks> calculate(Iterable<Streak> streaks, long watermarkTimestamp) {
        int bucketSize = getBucketSize(watermarkTimestamp);
        Map<Integer, Integer> streaksPerBucket = new HashMap<>();

        for (Streak streak : streaks) {
            int bucket = streak.getBucket(watermarkTimestamp, bucketSize);
            Integer count = streaksPerBucket.get(bucket);
            streaksPerBucket.put(bucket, count != null ? count + 1 : 1);
        }
        int totalStreaks = streaksPerBucket.values().stream().mapToInt(Integer::intValue).sum();

        List<TopKStreaks> topKStreaks = new ArrayList<>(14);
        //System.out.println("Batch: " + watermarkTimestamp);
        for (int i = 0; i < 14; i++) {
            Integer numberOfStreaks = streaksPerBucket.get(i);
            int percent;
            if (numberOfStreaks == null) {
                percent = 0;
            } else {
                percent = Math.round((float) numberOfStreaks / totalStreaks * 1000);
            }
            //System.out.println(i + ":" + " from: " + i * bucketSize + " to: " + (i + 1) * bucketSize + " percent: " + percent);
            topKStreaksBuilder.clear();
            TopKStreaks streak = topKStreaksBuilder
                    .setBucketFrom(i * bucketSize)
                    .setBucketTo((i + 1) * bucketSize)
                    .setBucketPercent(percent)
                    .build();
            topKStreaks.add(streak);
        }
        return topKStreaks;
    }
}
