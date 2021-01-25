package de.hpi.debs;

import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChallengerClient {

    private final ChallengerGrpc.ChallengerStub asyncStub;
    private final ChallengerGrpc.ChallengerBlockingStub blockingStub;

    public ChallengerClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    public ChallengerClient(ManagedChannelBuilder<?> channelBuilder) {
        ManagedChannel channel = channelBuilder
                .maxInboundMessageSize(100 * 1024 * 1024)
                .build();
        blockingStub = ChallengerGrpc.newBlockingStub(channel);
        asyncStub = ChallengerGrpc.newStub(channel);
    }

    public static void main(String[] args) {
        ChallengerClient client = new ChallengerClient("challenge.msrg.in.tum.de", 5023);
        BenchmarkConfiguration.Query query = BenchmarkConfiguration.Query.Q2;
        BenchmarkConfiguration configuration = BenchmarkConfiguration.newBuilder()
                .setToken(System.getenv("DEBS_API_KEY"))
                .setBatchSize(4)
                .setBenchmarkName("firstTest")
                .setBenchmarkType("test")
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .build();
        Benchmark benchmark = client.blockingStub.createNewBenchmark(configuration);
        Locations locations = client.blockingStub.getLocations(benchmark);
    }
}
