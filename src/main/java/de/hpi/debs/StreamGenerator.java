package de.hpi.debs;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Measurement;

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

        while (running) {
            Batch batch = Main.challengeClient.nextBatch(benchmark);

            if (batch.getLast()) { // Stop when we get the last batch
                // System.out.println("Received last batch, finished!");
                running = false;
                break;
            }

            // process the batch of events we have
            List<Measurement> currentYearList = batch.getCurrentList();
            List<Measurement> lastYearList = batch.getLastyearList();

            for (int i = 0; i < currentYearList.size() - 1; i++) {

                city = Main.locationRetriever.findCityForLocation(
                        new PointOwn(currentYearList.get(i))
                );

                if (city.isPresent()) {
                    context.collectWithTimestamp(
                            MeasurementOwn.fromMeasurement(currentYearList.get(i), city.get()),
                            currentYearList.get(i).getTimestamp().getSeconds()
                    );
                }
            }

            for (int i = 0; i < lastYearList.size() - 1; i++) {

                city = Main.locationRetriever.findCityForLocation(
                        new PointOwn(lastYearList.get(i))
                );

                if (city.isPresent()) {
                    context.collectWithTimestamp(
                            MeasurementOwn.fromMeasurement(lastYearList.get(i), city.get()),
                            lastYearList.get(i).getTimestamp().getSeconds()
                    );
                }
            }

            // emit watermark
            city = Main.locationRetriever.findCityForLocation(
                    new PointOwn(currentYearList.get(currentYearList.size() - 1))
            );

            context.collectWithTimestamp(
                    MeasurementOwn.fromMeasurement(currentYearList.get(currentYearList.size() - 1), city.orElse("no"), true),
                    currentYearList.get(currentYearList.size() - 1).getTimestamp().getSeconds()
            );

            context.emitWatermark(new Watermark(currentYearList.get(currentYearList.size() - 1).getTimestamp().getSeconds()));

            // System.out.println("Processed batch #" + cnt);
            ++cnt;

            if (cnt >= batchNumbers) { //for testing you can
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
