package de.hpi.debs.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class SplitSimpleVersionedSerializer implements SimpleVersionedSerializer<SourceSplitOwn> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(SourceSplitOwn obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        return bos.toByteArray();
    }

    @Override
    public SourceSplitOwn deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        ObjectInputStream is = new ObjectInputStream(in);
        SourceSplitOwn obj;
        try {
            obj = (SourceSplitOwn) is.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return new SourceSplitOwn();
        }
        return obj;
    }
}
