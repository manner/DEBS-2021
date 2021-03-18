package de.hpi.debs.aqi;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class AQICalculator {

    private static final Map<Float, Integer> AQI_25_Map = new HashMap<>() {{
        put(0f, 0);
        put(12f, 50);

        put(12.1f, 51);
        put(35.4f, 100);

        put(35.5f, 101);
        put(55.4f, 150);

        put(55.5f, 151);
        put(150.4f, 200);

        put(150.5f, 201);
        put(250.4f, 300);

        put(250.5f, 301);
        put(350.4f, 400);

        put(350.5f, 401);
        put(500.4f, 500);
    }};

    private static final Map<Float, Integer> AQI_10_Map = new HashMap<>() {{
        put(0f, 0);
        put(54f, 50);

        put(55f, 51);
        put(154f, 100);

        put(155f, 101);
        put(254f, 150);

        put(255f, 151);
        put(354f, 200);

        put(355f, 201);
        put(424f, 300);

        put(425f, 301);
        put(504f, 400);

        put(505f, 401);
        put(604f, 500);
    }};

    private static final NavigableMap<Float, Integer> AQI_10 = new TreeMap<>(AQI_10_Map);
    private static final NavigableMap<Float, Integer> AQI_25 = new TreeMap<>(AQI_25_Map);

    public static int getAQI25(float value) {
        float truncatedValue = (float) Math.floor(value * 10) / 10;
        return calculateAQI(truncatedValue, AQI_25);
    }

    public static int getAQI10(float value) {
        int truncatedValue = (int) Math.floor(value);
        return calculateAQI(truncatedValue, AQI_10);
    }

    private static int calculateAQI(float truncatedValue, NavigableMap<Float, Integer> map) {
        if (truncatedValue <= map.firstKey()) {
            return map.firstEntry().getValue();
        } else if (truncatedValue > map.lastKey()) {
            return map.lastEntry().getValue();
        }
        float BP_high = map.ceilingKey(truncatedValue);
        float BP_low = map.lowerKey(truncatedValue);
        int I_high = map.ceilingEntry(truncatedValue).getValue();
        int I_low = map.lowerEntry(truncatedValue).getValue();
        return Math.round((((I_high - I_low) / (BP_high - BP_low)) * (truncatedValue - BP_low)) + I_low);
    }

    public static int getAQI(float PM25, float PM10) {
        return Math.max(getAQI25(PM25), getAQI10(PM10));
    }
}
