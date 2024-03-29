package de.hpi.debs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.ChallengerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.concurrent.Executors;

public class AsyncStreamGenerator extends RichAsyncFunction<Long, Batch> {

    private final Benchmark benchmark;
    private boolean isStarted;
    private ListeningExecutorService executor;
    private ManagedChannel channel;
    private ChallengerGrpc.ChallengerFutureStub challengeClient;


    AsyncStreamGenerator(Benchmark benchmark) {
        this.benchmark = benchmark;
        this.isStarted = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                .usePlaintext()
                .build();

        challengeClient = ChallengerGrpc.newFutureStub(channel)
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
    }

    @Override
    public void close() {
        channel.shutdown();
    }

    @Override
    public void asyncInvoke(Long input, ResultFuture<Batch> resultFuture) {
        if (!isStarted) {
            System.out.println("starting Benchmark");
            challengeClient.startBenchmark(benchmark);
            isStarted = true;
        }
        ListenableFuture<Batch> listenableFuture = challengeClient.nextBatch(benchmark);

        Futures.addCallback(listenableFuture, new FutureCallback<>() {

            @Override
            public void onSuccess(Batch result) {
                resultFuture.complete(Collections.singletonList(result));
            }

            @Override
            public void onFailure(@NotNull Throwable t) {
                t.printStackTrace();
            }
        }, executor);
    }
}
