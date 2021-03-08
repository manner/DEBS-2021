package de.hpi.deps;

import de.hpi.debs.AQICalculator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AQICalculatorTests {

    @Test
    public void calculateCorrectAQI10() {
        assertEquals(73, AQICalculator.getAQI10(100f));
    }

    @Test
    public void calculateCorrectAQI25() {
        assertEquals(102, AQICalculator.getAQI25(35.9f));
    }

    @Test
    public void roundCorrectlyAQI25() {
        assertEquals(100, AQICalculator.getAQI25(35.49999f));
        assertEquals(101, AQICalculator.getAQI25(35.5f));
        assertEquals(400, AQICalculator.getAQI25(350.4f));
        assertEquals(401, AQICalculator.getAQI25(350.5f));
    }

    @Test
    public void roundCorrectlyAQI10() {
        assertEquals(50, AQICalculator.getAQI10(54.99f));
        assertEquals(51, AQICalculator.getAQI10(55.0f));
    }

    @Test
    public void testCornerCases() {
        assertEquals(0, AQICalculator.getAQI25(0f));
        assertEquals(0, AQICalculator.getAQI10(0f));

        assertEquals(500, AQICalculator.getAQI25(500.4f));
        assertEquals(500, AQICalculator.getAQI10(604f));

        assertEquals(500, AQICalculator.getAQI25(1000f));
        assertEquals(500, AQICalculator.getAQI10(1000f));

    }
}
