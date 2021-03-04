package de.hpi.deps;

import de.hpi.debs.ChallengerClient;
import de.hpi.debs.LocationRetriever;
import de.hpi.debs.PointOwn;
import de.tum.i13.bandency.Locations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocationRetrieverTests {

    LocationRetriever locationRetriever;

    @BeforeEach
    public void setupLocationRetriever() throws IOException {
        Locations locations = ChallengerClient.readLocationsFromFile("./locations.ser");
        locationRetriever = new LocationRetriever(locations);
    }

    @Test
    public void retrievesCorrectCity() {
        PointOwn p1 = new PointOwn(52.392513f, 13.088658f);
        PointOwn p2 = new PointOwn(49.036854f, 9.376519f);
        PointOwn p3 = new PointOwn(52.390536f, 13.128201f);

        String city1 = locationRetriever.findCityForLocation(p1);
        String city2 = locationRetriever.findCityForLocation(p2);
        String city3 = locationRetriever.findCityForLocation(p3);


        assertEquals("Potsdam", city1);
        assertEquals("Oberstenfeld", city2);
        assertEquals("Berlin Wannsee", city3);

    }

}
