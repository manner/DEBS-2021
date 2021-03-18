package de.hpi.debs;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Measurement;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.List;
import java.util.Optional;

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

        Optional<String> city;

        while(running) {
            Batch batch = Main.challengeClient.nextBatch(benchmark);

            if (batch.getLast()) { // Stop when we get the last batch
                // System.out.println("Received last batch, finished!");
                running = false;
                break;
            }

            // process the batch of events we have
            List<Measurement> mList = batch.getCurrentList();

            for (int i = 0; i < mList.size() - 1; i++) {

                city = Main.locationRetriever.findCityForLocation(
                        new PointOwn(mList.get(i).getLatitude(), mList.get(i).getLongitude())
                );

                if (city.isPresent())
                    context.collectWithTimestamp(
                            MeasurementOwn.fromMeasurement(mList.get(i), city.get()),
                            mList.get(i).getTimestamp().getSeconds()
                    );
            }

            // emit watermark
            city = Main.locationRetriever.findCityForLocation(
                    new PointOwn(
                            mList.get(mList.size() - 1).getLatitude(),
                            mList.get(mList.size() - 1).getLongitude()
                    )
            );

            if (city.isPresent()) {

                context.collectWithTimestamp(
                        MeasurementOwn.fromMeasurement(mList.get(mList.size() - 1), city.get()),
                        mList.get(mList.size() - 1).getTimestamp().getSeconds()
                );
            } else {

                context.collectWithTimestamp(
                        MeasurementOwn.fromMeasurement(mList.get(mList.size() - 1), "no"),
                        mList.get(mList.size() - 1).getTimestamp().getSeconds()
                );
            }
            context.emitWatermark(new Watermark(mList.get(mList.size() - 1).getTimestamp().getSeconds()));

            // System.out.println("Processed batch #" + cnt);
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
