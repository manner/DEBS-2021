package de.hpi.debs.serializer;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.ChallengerGrpc;
import de.tum.i13.bandency.Locations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class BatchSerializer {
    public static Batch getBatch(ChallengerGrpc.ChallengerBlockingStub client, Benchmark benchmark, int n) {
        Batch batch;
        String locationFileName = "./batch" + n + ".ser";
        if (new File(locationFileName).isFile()) {
            batch = readBatchFromFile(locationFileName);
        } else {
            batch = client.nextBatch(benchmark);
            saveBatchToFile(locationFileName, batch);
        }
        return batch;
    }

    public static Batch readBatchFromFile(String locationFileName) {
        Batch batch = null;
        try (
                FileInputStream streamIn = new FileInputStream(locationFileName);
                ObjectInputStream objectinputstream = new ObjectInputStream(streamIn)
        ) {
            batch = (Batch) objectinputstream.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return batch;
    }

    private static void saveBatchToFile(String locationFileName, Batch batch) {
        try (
                FileOutputStream fos = new FileOutputStream(locationFileName, true);
                ObjectOutputStream oos = new ObjectOutputStream(fos)
        ) {
            oos.writeObject(batch);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
