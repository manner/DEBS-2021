package de.hpi.debs;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Measurement;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;

public class StreamGenerator implements SourceFunction<MeasurementOwn> {
    private volatile boolean running = true;
    private int cnt = 0;
    private int batchNumbers;
    private Benchmark benchmark;

    public StreamGenerator(
            Benchmark benchmarkIn,
            int batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        batchNumbers = batchNumbersIn;
    }

    @Override
    public void run(SourceContext<MeasurementOwn> context) {

        while(running) {
            Batch batch = Main.challengeClient.nextBatch(benchmark);

            if (batch.getLast()) { //Stop when we get the last batch
                System.out.println("Received last batch, finished!");
                running = false;
                break;
            }

            //process the batch of events we have
            List<Measurement> mList = batch.getCurrentList();

            for (Measurement m : mList)
                context.collectWithTimestamp(MeasurementOwn.fromMeasurement(m), m.getTimestamp().getSeconds());

            context.emitWatermark(new Watermark((mList.get(mList.size() - 1).getTimestamp().getSeconds())));

            System.out.println("Processed batch #" + cnt);
            ++cnt;

            if(cnt >= batchNumbers) { //for testing you can
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
