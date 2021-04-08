package de.hpi.debs;

import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.Serializable;

public class SpatialIndexFeatureCollectionOwn extends SpatialIndexFeatureCollection implements Serializable {
    public SpatialIndexFeatureCollectionOwn(SimpleFeatureType schema) {
        super(schema);
    }
}
