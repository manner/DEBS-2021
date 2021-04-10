package de.hpi.debs;

import de.tum.i13.bandency.Batch;
import io.grpc.stub.StreamObserver;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.HashMap;

public class StreamObserverOwn implements StreamObserver<Batch>, Serializable {
    protected SourceFunction.SourceContext<Batch> context;
    protected final long cnt;
    protected long n;
    protected long seq;
    protected long compTS;
    protected boolean end;
    protected HashMap<Long, Batch> batchBuffer;

    public StreamObserverOwn(long capacity) {
        super();

        cnt = Long.MAX_VALUE;
        n = capacity;
        seq = 0;
        compTS = 0;
        end = false;
        batchBuffer = new HashMap<>();
        for(long i = 0; i <= n; i++) {
            batchBuffer.put(i, null);
        }
    }

    public StreamObserverOwn(long capacity, long numberOfBatches) {
        super();

        cnt = numberOfBatches;
        n = capacity;
        seq = 0;
        compTS = 0;
        end = false;
        batchBuffer = new HashMap<>();
        for(long i = 0; i <= n; i++) {
            batchBuffer.put(i, null);
        }
    }

    public void setContext(SourceFunction.SourceContext<Batch> context) {
        this.context = context;
    }

    public synchronized long syncOperations(Batch batch, long ts, int operation) {
            long relSeq = seq % n;

            if (operation == 2) { // set completion ts
                compTS = ts;
                end = true;
            } else if (operation == 0 && batch != null) { // regular received batch processing
                if (batch.getSeqId() == seq && seq < cnt) {
                    context.collect(batch);
                    seq++;
                    relSeq = seq % n;

                    Batch out = batchBuffer.get(relSeq);
                    while (out != null && out.getSeqId() == seq && seq < cnt) {
                        context.collect(out);
                        seq++;
                        relSeq = seq % n;
                        out = batchBuffer.get(relSeq);
                    }
                } else {
                    batchBuffer.put(batch.getSeqId() % n, batch);

                    Batch out = batchBuffer.get(relSeq);
                    while (out != null && out.getSeqId() == seq && seq < cnt) {
                        context.collect(out);
                        seq++;
                        relSeq = seq % n;
                        out = batchBuffer.get(relSeq);
                    }
                }

                if (batch.getLast()) // start end timeout when last batch was received
                    syncOperations(null, System.currentTimeMillis(), 2);
            }

        if (operation == 1 && end) { // check if benchmark was ended
            if (10000 < System.currentTimeMillis() - compTS) // time out after completion to give pending nextBatch() time to finish
                return -2;
            return -1;
        }

        if (!end && cnt <= seq) // artificial and for testing
            syncOperations(null, System.currentTimeMillis(), 2);

        return seq;
    }

    @Override
    public void onNext(Batch batch) {
        syncOperations(batch, 0, 0);
    }

    @Override
    public void onError(Throwable t) {
        System.out.println("Cannot get request: " + t);
    }

    @Override
    public void onCompleted() {
    }
}
