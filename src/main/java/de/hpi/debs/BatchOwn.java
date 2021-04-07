package de.hpi.debs;

import de.tum.i13.bandency.Batch;

import java.io.Serializable;
import java.util.List;

public class BatchOwn implements Serializable {
    long seq_id;
    boolean last;
    List<MeasurementOwn> currentYear;
    List<MeasurementOwn> lastYear;

    public BatchOwn(long seq_id, boolean last, List<MeasurementOwn> currentYear, List<MeasurementOwn> lastYear) {
        this.seq_id = seq_id;
        this.last = last;
        this.currentYear = currentYear;
        this.lastYear = lastYear;
    }

    public static BatchOwn from(Batch batch) {
        return new BatchOwn(
                batch.getSeqId(),
                batch.getLast(),
                MeasurementOwn.fromMeasurements(batch.getCurrentList()),
                MeasurementOwn.fromMeasurements(batch.getLastyearList())
        );
    }

}
