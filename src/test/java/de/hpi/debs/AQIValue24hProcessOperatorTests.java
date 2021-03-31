package de.hpi.debs;

import de.hpi.debs.aqi.AQICalculator;
import de.hpi.debs.aqi.AQIValue24h;
import de.hpi.debs.aqi.AQIValue24hProcessOperator;
import de.hpi.debs.testHarness.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AQIValue24hProcessOperatorTests {

    public static class AQIValue24hProcessOperatorTestClass extends AQIValue24hProcessOperator {

        public AQIValue24hProcessOperatorTestClass(long start) {
            super(start);
        }
    }

    protected ArrayList<TestEvent> events = new ArrayList<>() {{
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 3000, "Poland", false), 3000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 99000,  "Berlin", false), 99000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 200000, "Berlin", false), 200000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 250000, "Poland", false), 250000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 300000, "Berlin", false), 300000));
        add(new TestEvent(null, 300000)); // watermarks have data of null
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 511000, "Berlin", false), 511000));
        add(new TestEvent(null, 600000));
        add(new TestEvent(null, 950000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 511000, "Berlin", false), 511000));
        add(new TestEvent(null, 1200000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1501000, "Berlin", false), 1501000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1531000, "Poland", false), 1531000));
        add(new TestEvent(null, 1500000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1601000, "Berlin", false), 1601000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1001000, "Berlin", false), 1001000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1561000, "Poland", false), 1561000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1551000, "Berlin", false), 1551000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611000, "Berlin", false), 1611000));
        add(new TestEvent(null, 1900000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611000,  "Berlin", false), 1611000));
        add(new TestEvent(new MeasurementOwn(10000.0F, 2.0F, 0, 0, 2001000, "Berlin", false), 2001000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000000, "Poland", false), 2000000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2100000, "Poland", false), 2100000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2030000, "Poland", false), 2030000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2110000, "Poland", false), 2110000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1999000, "Poland", false), 1999000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000000, "Poland", false), 2000000));
        add(new TestEvent(null, 2200000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2001000, "Poland", false), 2001000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2201000, "Berlin", false), 2201000));
        add(new TestEvent(null, 2900000));
    }};

    // !attention! only one watermark for last key is emitted for all partitions, in this case we have only one partition
    protected ArrayList<AQIValue24h> groundTruthBerlin = new ArrayList<>() {{
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 300000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 600000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 900000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 950000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1200000, false, "Berlin", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1500000, false, "Berlin", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1900000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2100000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2200000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2400000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2700000, false, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2900000, false, "Berlin", false));
    }};

    protected ArrayList<AQIValue24h> groundTruthPoland = new ArrayList<>() {{
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 300000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 600000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 900000, false, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 950000, false, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1200000, false, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1500000, false, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1900000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2100000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2200000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2400000, false, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2700000, false, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2900000, false, "Poland", false));
    }};

    private static class EventKeySelector implements KeySelector<MeasurementOwn, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(MeasurementOwn e) {
            return e.getCity();
        }
    }

    @Test
    public void multipleCitiesTest() throws Exception {
        AQIValue24hProcessOperatorTestClass operator = new AQIValue24hProcessOperatorTestClass(0);

        KeyedOneInputStreamOperatorTestHarness<String, MeasurementOwn, AQIValue24h> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (TestEvent e : events) {
                if (e.getData() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTs()));
                } else { // otherwise emit event
                    testHarness.processElement(new StreamRecord<>(e.getData(), e.getTs()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e.toString(), "");
        }

        int iP = 0;
        int iB = 0;
        AQIValue24h tmp;

        for (Object item : testHarness.getOutput()) {
            tmp = ((StreamRecord<AQIValue24h>) item).getValue();

            if (tmp.getCity().equals("Poland")) {
                assertEquals(groundTruthPoland.get(iP).getAQI(), tmp.getAQI());
                assertEquals(groundTruthPoland.get(iP).getTimestamp(), tmp.getTimestamp());
                assertEquals(groundTruthPoland.get(iP).isActive(), tmp.isActive());
                iP++;
            } else if (tmp.getCity().equals("Berlin")) {
                assertEquals(groundTruthBerlin.get(iB).getAQI(), tmp.getAQI());
                assertEquals(groundTruthBerlin.get(iB).getTimestamp(), tmp.getTimestamp());
                assertEquals(groundTruthBerlin.get(iB).isActive(), tmp.isActive());
                iB++;
            } else {
                assertEquals("Lul! Dat city not exist! Should exist!", "");
            }
        }

        testHarness.close();
    }

    @Test
    public void Test() {
    }
}
