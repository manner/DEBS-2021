package de.hpi.deps;

import de.hpi.debs.AQI;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AQITests {

    @Test
    public void calculateCorrectAQI10() {
        assertEquals(73, AQI.getAQI10(100f));
    }

    @Test
    public void calculateCorrectAQI25() {
        assertEquals(102, AQI.getAQI25(35.9f));
    }

    @Test
    public void roundCorrectlyAQI25() {
        assertEquals(100, AQI.getAQI25(35.49999f));
        assertEquals(101, AQI.getAQI25(35.5f));
        assertEquals(400, AQI.getAQI25(350.4f));
        assertEquals(401, AQI.getAQI25(350.5f));
    }

    @Test
    public void roundCorrectlyAQI10() {
        assertEquals(50, AQI.getAQI10(54.99f));
        assertEquals(51, AQI.getAQI10(55.0f));
    }

    @Test
    public void testCornerCases() {
        assertEquals(0, AQI.getAQI25(0f));
        assertEquals(0, AQI.getAQI10(0f));

        assertEquals(500, AQI.getAQI25(500.4f));
        assertEquals(500, AQI.getAQI10(604f));

        assertEquals(500, AQI.getAQI25(1000f));
        assertEquals(500, AQI.getAQI10(1000f));

    }
}
