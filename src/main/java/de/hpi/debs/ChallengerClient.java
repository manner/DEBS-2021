package de.hpi.debs;

import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.BenchmarkConfiguration;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.awt.Polygon;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;

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
        BenchmarkConfiguration configuration = BenchmarkConfiguration.newBuilder()
                .setToken(System.getenv("DEBS_API_KEY"))
                .setBatchSize(100)
                .setBenchmarkName("Testrun " + new Date().toString())
                .setBenchmarkType("test")
                .addQueries(BenchmarkConfiguration.Query.Q1)
                .build();

        Benchmark benchmark = client.blockingStub.createNewBenchmark(configuration);

        Locations locations = getLocations(client, benchmark);
    }

    private static Locations getLocations(ChallengerClient client, Benchmark benchmark) {
        Locations locations;
        String locationFileName = "./locations.ser";
        if (new File(locationFileName).isFile()) {
            locations = readLocationsFromFile(locationFileName);
        } else {
            locations = client.blockingStub.getLocations(benchmark);
            saveLocationsToFile(locationFileName, locations);
        }
        return locations;
    }

    private static Locations readLocationsFromFile(String locationFileName) {
        Locations locations = null;
        try (
                FileInputStream streamIn = new FileInputStream(locationFileName);
                ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
        ) {
            locations = (Locations) objectinputstream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return locations;
    }

    private static void saveLocationsToFile(String locationFileName, Locations locations) {
        try (
                FileOutputStream fout = new FileOutputStream(locationFileName, true);
                ObjectOutputStream oos = new ObjectOutputStream(fout);
        ) {
            oos.writeObject(locations);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
