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
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 3, "Poland"), 3));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 99, "Berlin"), 99));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 200, "Berlin"), 200));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 250, "Poland"), 250));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 300, "Berlin"), 300));
        add(new TestEvent(null, 300)); // watermarks have data of null
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 511, "Berlin"), 511));
        add(new TestEvent(null, 600));
        add(new TestEvent(null, 950));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 511, "Berlin"), 511));
        add(new TestEvent(null, 1200));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1501, "Berlin"), 1501));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1531, "Poland"), 1531));
        add(new TestEvent(null, 1500));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1601, "Berlin"), 1601));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1001, "Berlin"), 1001));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1561, "Poland"), 1561));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1551, "Berlin"), 1551));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611, "Berlin"), 1611));
        add(new TestEvent(null, 1900));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611, "Berlin"), 1611));
        add(new TestEvent(new MeasurementOwn(10000.0F, 2.0F, 0, 0, 2001, "Berlin"), 2001));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000, "Poland"), 2000));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2100, "Poland"), 2100));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2030, "Poland"), 2030));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2110, "Poland"), 2110));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1999, "Poland"), 1999));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000, "Poland"), 2000));
        add(new TestEvent(null, 2200));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2001, "Poland"), 2001));
        add(new TestEvent(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2201, "Berlin"), 2201));
        add(new TestEvent(null, 2900));
    }};

    // !attention! only one watermark for last key is emitted for all partitions, in this case we have only one partition
    protected ArrayList<AQIValue24h> groundTruthBerlin = new ArrayList<>() {{
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 300, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 600, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 900, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 950, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1200, "Berlin", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1500, "Berlin", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1900, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2100, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2400, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2700, "Berlin", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 10000), 2900, "Berlin", false));
    }};

    protected ArrayList<AQIValue24h> groundTruthPoland = new ArrayList<>() {{
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 300, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 600, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 900, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1200, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1500, "Poland", false));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2100, "Poland", true));
        add(new AQIValue24h(AQICalculator.getAQI(2, 1), 2200, "Poland", true));
    }};

    private static class EventKeySelector implements KeySelector<MeasurementOwn, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(MeasurementOwn e) {
            return e.getCity();
        }
    }

    @Test
    public void testRun() throws Exception {
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
}
