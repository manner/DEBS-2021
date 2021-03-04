package de.hpi.debs;

import de.tum.i13.bandency.Location;
import de.tum.i13.bandency.Locations;
import de.tum.i13.bandency.Point;
import org.geotools.data.DataStore;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.linearref.LinearLocation;
import org.locationtech.jts.linearref.LocationIndexedLine;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.BoundingBox;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class LocationRetriever {

    private static final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
    private final SpatialIndexFeatureCollection index;
    private SimpleFeature lastMatched;
    private DefaultFeatureCollection featureCollection;

    public LocationRetriever(Locations locations) throws IOException {
        createDataStore(locations);

        index = new SpatialIndexFeatureCollection(featureCollection.getSchema());
        index.addAll(featureCollection.collection());

    }

    private static Function<Location, SimpleFeature>
    toFeature(SimpleFeatureType CITY, GeometryFactory geometryFactory) {
        return location -> {
            List<de.tum.i13.bandency.Polygon> polygonList = location.getPolygonsList();
            Polygon[] polygons = new Polygon[polygonList.size()];
            for (int i = 0; i < polygonList.size(); i++ ) {
                Coordinate[] coordinates = getCoordinates(polygonList.get(i).getPointsList());
                polygons[i] = geometryFactory.createPolygon(coordinates);
            }
            MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(polygons);

            SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(CITY);
            featureBuilder.add(multiPolygon);
            featureBuilder.add(location.getCity());
            featureBuilder.add(location.getZipcode());
            return featureBuilder.buildFeature(null);
        };

    }


    private static Coordinate[] getCoordinates(List<de.tum.i13.bandency.Point> points) {
        return points.stream()
                .map(LocationRetriever::pointToCoordinate)
                .toArray(Coordinate[]::new);
    }

    private static Coordinate pointToCoordinate(de.tum.i13.bandency.Point point) {
        return new Coordinate(point.getLatitude(), point.getLongitude());
    }

    private void createDataStore(Locations locations) throws IOException {
        SimpleFeatureTypeBuilder featureTypeBuilder = new SimpleFeatureTypeBuilder();
        featureTypeBuilder.setName("Location");
        featureTypeBuilder.add("polygon", MultiPolygon.class);
        featureTypeBuilder.add("city", String.class);
        featureTypeBuilder.add("plz", String.class);
        SimpleFeatureType CITY = featureTypeBuilder.buildFeatureType();

        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(CITY);
        featureCollection = new DefaultFeatureCollection();
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

        locations.getLocationsList().stream()
                .map(toFeature(CITY, geometryFactory))
                .forEach(featureCollection::add);

        File shapeFile = new File(
                new File(".").getAbsolutePath() + "shapefile.shp");

        Map<String, Serializable> params = new HashMap<>();
        params.put("url", shapeFile.toURI().toURL());
        params.put("create spatial index", Boolean.TRUE);

        ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();

        DataStore ds = dataStoreFactory.createNewDataStore(params);
        ds.createSchema(CITY);
    }

    public Optional<String> findNearestPolygon(Coordinate coordinate) {
        final double MAX_SEARCH_DISTANCE = index.getBounds().getSpan(0);
        ReferencedEnvelope search = new ReferencedEnvelope(new Envelope(coordinate),
                index.getSchema().getCoordinateReferenceSystem());
        search.expandBy(MAX_SEARCH_DISTANCE);
        BBOX bbox = ff.bbox(ff.property(index.getSchema().getGeometryDescriptor().getName()), (BoundingBox) search);
        SimpleFeatureCollection candidates = index.subCollection(bbox);

        double minDist = MAX_SEARCH_DISTANCE + 1.0e-6;
        Coordinate minDistPoint = null;
        try (SimpleFeatureIterator itr = candidates.features()) {
            while (itr.hasNext()) {
                SimpleFeature feature = itr.next();
                LocationIndexedLine line = new LocationIndexedLine(((MultiPolygon) feature.getDefaultGeometry()).getBoundary());
                LinearLocation here = line.project(coordinate);
                Coordinate point = line.extractPoint(here);
                double dist = point.distance(coordinate);
                if (dist < minDist) {
                    minDist = dist;
                    minDistPoint = point;
                    lastMatched = feature;
                }
            }
        }
        if (minDistPoint == null) {
            return Optional.empty();
        } else {
            return Optional.of((String) lastMatched.getAttribute("city"));
        }
    }

    public String findCityForLocation(Point point) {
        return findNearestPolygon(pointToCoordinate(point))
                .orElse("Point not found");
    }

    public String findCityForLocation(PointOwn point) {
        return findNearestPolygon(new Coordinate(point.getLatitude(), point.getLongitude()))
                .orElse("Point not found");
    }
}
