package de.hpi.debs.source;

import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointType;

public class SourceOwn implements Source<Batch, SourceSplitOwn, CheckpointType> {
    protected Benchmark benchmark;
    protected long batchNumbers;
    protected SplitEnumerator<SourceSplitOwn, CheckpointType> splitEnumerator;

    public SourceOwn(Benchmark benchmark, long batchNumbers) {
        this.benchmark = benchmark;
        this.batchNumbers = batchNumbers;
    }

    public SourceOwn(Benchmark benchmark) {
        this.benchmark = benchmark;
        this.batchNumbers = Long.MAX_VALUE;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Batch, SourceSplitOwn> createReader(SourceReaderContext readerContext) {
        return new SourceReaderOwn(readerContext, benchmark, batchNumbers);
    }

    @Override
    public SplitEnumerator<SourceSplitOwn, CheckpointType> createEnumerator(SplitEnumeratorContext<SourceSplitOwn> enumContext) {
        splitEnumerator = new SplitEnumeratorOwn();
        return splitEnumerator;
    }

    @Override
    public SplitEnumerator<SourceSplitOwn, CheckpointType> restoreEnumerator(SplitEnumeratorContext<SourceSplitOwn> enumContext, CheckpointType checkpoint) {
        return splitEnumerator;
    }

    @Override
    public SimpleVersionedSerializer<SourceSplitOwn> getSplitSerializer() {
        return new SplitSimpleVersionedSerializer();
    }

    @Override
    public SimpleVersionedSerializer<CheckpointType> getEnumeratorCheckpointSerializer() {
        return new TypeSimpleVersionedSerializer();
    }
}
