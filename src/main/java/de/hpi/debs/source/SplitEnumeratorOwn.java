package de.hpi.debs.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

public class SplitEnumeratorOwn implements SplitEnumerator<SourceSplitOwn, CheckpointType> {
    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<SourceSplitOwn> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public CheckpointType snapshotState() {
        return CheckpointType.CHECKPOINT;
    }

    @Override
    public void close() throws IOException {

    }
}
