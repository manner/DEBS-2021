package de.hpi.debs.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;

public class SourceSplitOwn implements SourceSplit, Serializable {
    int i = 0;
    @Override
    public String splitId() {
        return "SourceOwn-" + i++;
    }
}
