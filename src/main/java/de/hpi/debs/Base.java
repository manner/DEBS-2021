package de.hpi.debs;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Source generator translating batches into a source stream.
 */
public class Base {
    public static SourceFunction<MeasurementOwn> measurements = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;

    /**
     * Retrieves a test source during unit tests and the given one during normal execution.
     */
    public static SourceFunction<MeasurementOwn> measurementsSourceOrTest(SourceFunction<MeasurementOwn> source) {
        if (measurements == null) {
            return source;
        }
        return measurements;
    }

    /**
     * Prints the given data stream during normal execution and collects outputs during tests.
     */
    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}
