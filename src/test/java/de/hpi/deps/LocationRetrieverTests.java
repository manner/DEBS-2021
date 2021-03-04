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
        PointOwn potsdam = new PointOwn(52.391879f, 13.092486f);
        String city = locationRetriever.findCityForLocation(potsdam);
        assertEquals("Potsdam", city);
    }

}
