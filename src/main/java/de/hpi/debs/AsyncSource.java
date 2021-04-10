package de.hpi.debs;

import de.tum.i13.bandency.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AsyncSource implements SourceFunction<Batch> {

    private static ChallengerGrpc.ChallengerStub challengeClient;
    private volatile boolean running = true;
    private final long numberOfBatches;
    private final Benchmark benchmark;
    private StreamObserverOwn observer;
    private long capacity;
    private long seq;
    private long status;
    private long nextM;
    private long requested;

    public AsyncSource(
            Benchmark benchmarkIn,
            long capacity
    ) {

        benchmark = benchmarkIn;
        numberOfBatches = Long.MAX_VALUE;
        observer = new StreamObserverOwn(capacity);
        this.capacity = capacity;
        this.seq = 0;
        nextM = capacity;
        requested = 0;

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        //for demo, we show the blocking stub
        challengeClient = ChallengerGrpc.newStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
    }

    public AsyncSource(
            Benchmark benchmarkIn,
            long capacity,
            long batchNumbersIn
    ) {

        benchmark = benchmarkIn;
        numberOfBatches = batchNumbersIn;
        observer = new StreamObserverOwn(capacity, batchNumbersIn);
        this.capacity = capacity;
        this.seq = 0;
        nextM = capacity;
        requested = 0;

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        //for demo, we show the blocking stub
        challengeClient = ChallengerGrpc.newStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

    }

    @Override
    public void run(SourceContext<Batch> context) throws Exception {
        observer.setContext(context);

        while (0 <= seq) {
            for (int i = 0; i < nextM; i++) {
                challengeClient.nextBatch(benchmark, observer);
            }

            status = observer.syncOperations(null, 0, 1);

            if (!running)
                observer.syncOperations(null, System.currentTimeMillis(), 2);

            if (0 <= status) { // regular case compute how many batches are finished and therefore how many more can be requested
                requested += nextM;
                nextM = status - seq;
                if (numberOfBatches < requested + nextM)
                    nextM = numberOfBatches - requested;
                seq = status;
            }
            else if (status == -2) { // end of data in
                nextM = 0;
                seq = status;
            } else if (status == -1) {
                nextM = 0;
            } else { // no more data needed, but a result not equal to -1 is not suppose to happen
                throw new Exception("StreamObserverOwn is broken.");
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
