package de.hpi.debs.source;

import de.tum.i13.bandency.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SourceReaderOwn implements SourceReader<Batch, SourceSplitOwn> {
    protected SourceReaderContext context;
    protected static ChallengerGrpc.ChallengerBlockingStub challengeClient;
    protected long requested = 0;
    protected final long batchNumbers;
    protected final Benchmark benchmark;
    protected List<SourceSplitOwn> snapshotState;
    protected boolean startup;
    protected boolean end;

    public SourceReaderOwn(
            SourceReaderContext context,
            Benchmark benchmark,
            long batchNumbers
    ){
        this.context = context;
        this.benchmark = benchmark;
        this.batchNumbers = batchNumbers;
        this.snapshotState = null;
        this.end = false;
        this.startup = true;
    }

    @Override
    public void start() {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        //for demo, we show the blocking stub
        challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
    }

    @Override
    public InputStatus pollNext(ReaderOutput<Batch> output) {
        Batch batch = null;

        if (startup) {
            System.out.println("starting Benchmark");
            System.out.println(challengeClient.startBenchmark(benchmark));
            startup = false;
        }

        if (!end) {
            batch = challengeClient.nextBatch(benchmark);
            output.collect(batch);
            requested++;
        }

        if (end || batch.getLast() || batchNumbers <= requested) {
            end = false;
            return InputStatus.END_OF_INPUT;
        }
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<SourceSplitOwn> snapshotState(long checkpointId) {
        if (snapshotState != null)
            return snapshotState;
        return new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return new CompletableFuture<>();
    }

    @Override
    public void addSplits(List<SourceSplitOwn> splits) {
        if (snapshotState != null)
            snapshotState.addAll(splits);

        snapshotState = new ArrayList<>(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
    }

    @Override
    public void close() throws Exception {
    }
}
