package de.hpi.debs.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointType;

import java.io.*;

public class TypeSimpleVersionedSerializer implements SimpleVersionedSerializer<CheckpointType> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(CheckpointType obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        return bos.toByteArray();
    }

    @Override
    public CheckpointType deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        ObjectInputStream is = new ObjectInputStream(in);
        CheckpointType obj;
        try {
            obj = (CheckpointType) is.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return CheckpointType.CHECKPOINT;
        }
        return obj;
    }
}
