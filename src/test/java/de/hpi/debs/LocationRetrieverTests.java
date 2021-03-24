package de.hpi.debs;

import de.hpi.debs.serializer.LocationSerializer;
import de.tum.i13.bandency.Locations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocationRetrieverTests {

    LocationRetriever locationRetriever;

    @BeforeEach
    public void setupLocationRetriever() throws IOException {
        Locations locations = LocationSerializer.readLocationsFromFile("./locations.ser");
        locationRetriever = new LocationRetriever(locations);
    }

    @Test
    public void retrievesCorrectCity() {
        PointOwn p1 = new PointOwn(52.392513f, 13.088658f);
        PointOwn p2 = new PointOwn(49.036854f, 9.376519f);
        PointOwn p3 = new PointOwn(52.390536f, 13.128201f);

        Optional<String> city1 = locationRetriever.findCityForLocation(p1);
        Optional<String> city2 = locationRetriever.findCityForLocation(p2);
        Optional<String> city3 = locationRetriever.findCityForLocation(p3);


        assertEquals(Optional.of("Potsdam"), city1);
        assertEquals(Optional.of("Oberstenfeld"), city2);
        assertEquals(Optional.of("Berlin Wannsee"), city3);

    }

}
