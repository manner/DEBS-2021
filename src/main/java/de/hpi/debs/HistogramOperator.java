package de.hpi.debs;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import de.hpi.debs.aqi.LongestStreakProcessor;
import de.tum.i13.bandency.ResultQ2;
import de.tum.i13.bandency.TopKStreaks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistogramOperator extends ProcessOperator<LongestStreakProcessor.Streak, Void> {

    private final long benchmarkId;
    protected ListState<LongestStreakProcessor.Streak> streaks;
    private int seqCounter;

    public HistogramOperator(long benchmarkId) {
        super(new ProcessFunction<>() {
            @Override
            public void processElement(LongestStreakProcessor.Streak value, Context ctx, Collector<Void> out) {
                // do nothing as we are doing everything in the operator
            }
        });
        this.benchmarkId = benchmarkId;
        this.seqCounter = 0;
    }

    @Override
    public void open() throws Exception {
        ListStateDescriptor<LongestStreakProcessor.Streak> descriptor =
                new ListStateDescriptor<>(
                        "streaks",
                        LongestStreakProcessor.Streak.class);

        streaks = getOperatorStateBackend().getListState(descriptor);
    }


    @Override
    public void processElement(StreamRecord<LongestStreakProcessor.Streak> value) throws Exception {
        streaks.add(value.getValue());
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        List<TopKStreaks> topKStreaks = calculate(streaks.get(), mark.getTimestamp());
        ResultQ2 result = ResultQ2.newBuilder()
                .addAllHistogram(topKStreaks)
                .setBatchSeqId(seqCounter++) // TODO: FIX THIS!
                .setBenchmarkId(benchmarkId)
                .build();
        Main.challengeClient.resultQ2(result);
        streaks.clear();
    }

    private int getBucketSize(long watermarkTimestamp) {
        long firstTimestampInBatch = 1583020800000L; // TODO: FIX THIS!
        long bucketSize = Math.max(0, (watermarkTimestamp - firstTimestampInBatch) / 14);
        return (int) Math.min(bucketSize, Time.days(7).toMilliseconds());
    }

    private List<TopKStreaks> calculate(Iterable<LongestStreakProcessor.Streak> streaks, long watermarkTimestamp) {
        int bucketSize = getBucketSize(watermarkTimestamp);
        Map<Integer, Integer> streaksPerBucket = new HashMap<>();

        for (LongestStreakProcessor.Streak streak : streaks) {
            int bucket = streak.getBucket(watermarkTimestamp, bucketSize);
            Integer count = streaksPerBucket.get(bucket);
            streaksPerBucket.put(bucket, count != null ? count + 1 : 1);
        }
        int totalStreaks = streaksPerBucket.values().stream().mapToInt(Integer::intValue).sum();

        List<TopKStreaks> topKStreaks = new ArrayList<>(14);
        for (int i = 0; i < 14; i++) {
            Integer numberOfStreaks = streaksPerBucket.get(i);
            int percent;
            if (numberOfStreaks == null) {
                percent = 0;
            } else {
                percent = Math.round((float) numberOfStreaks / totalStreaks * 1000);
            }
            System.out.println(i + ":" + " from: " + i * bucketSize + " to: " + (i + 1) * bucketSize + " percent: " + percent);
            TopKStreaks streak = TopKStreaks.newBuilder()
                    .setBucketFrom(i * bucketSize)
                    .setBucketTo((i + 1) * bucketSize)
                    .setBucketPercent(percent)
                    .build();
            topKStreaks.add(streak);
        }
        return topKStreaks;
    }
}
