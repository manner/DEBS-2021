package de.hpi.debs.source;

import de.hpi.debs.MeasurementOwn;
import de.tum.i13.bandency.Batch;
import de.tum.i13.bandency.Benchmark;
import de.tum.i13.bandency.Locations;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointType;

public class SourceOwn implements Source<MeasurementOwn, SourceSplitOwn, CheckpointType> {
    protected Benchmark benchmark;
    protected Locations locations;
    protected long batchNumbers;
    protected SplitEnumerator<SourceSplitOwn, CheckpointType> splitEnumerator;

    public SourceOwn(Benchmark benchmark, Locations locations, long batchNumbers) {
        this.benchmark = benchmark;
        this.locations = locations;
        this.batchNumbers = batchNumbers;
    }

    public SourceOwn(Benchmark benchmark, Locations locations) {
        this.benchmark = benchmark;
        this.locations = locations;
        this.batchNumbers = Long.MAX_VALUE;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<MeasurementOwn, SourceSplitOwn> createReader(SourceReaderContext readerContext) {
        return new SourceReaderOwn(readerContext, benchmark, locations, batchNumbers);
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
