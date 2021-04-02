package de.hpi.debs;

import de.hpi.debs.aqi.AQICalculator;
import de.hpi.debs.aqi.AQIValue24h;
import de.hpi.debs.aqi.AQIValue24hProcessOperator;
import de.hpi.debs.slicing.Slice;
import de.hpi.debs.testHarness.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AQIValue24hProcessOperatorTests {

    public static class AQIValue24hProcessOperatorTestClass extends AQIValue24hProcessOperator {

        public AQIValue24hProcessOperatorTestClass(long start) {
            super(start);
        }
    }

    protected ArrayList<StreamRecord<MeasurementOwn>> events = new ArrayList<>() {{
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 3000, "Poland", false), 3000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 99000, "Berlin", false), 99000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 200000, "Berlin", false), 200000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 250000, "Poland", false), 250000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 300000, "Berlin", false), 300000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 300000, "Berlin", true), 300000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 300000, "Poland", true), 300000));
        add(new StreamRecord<>(null, 300000)); // watermarks have data of null
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 511000, "Berlin", false), 511000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 600000, "Poland", true), 600000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 600000, "Berlin", true), 600000));
        add(new StreamRecord<>(null, 600000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 950000, "Berlin", true), 950000));
        add(new StreamRecord<>(null, 950000));
        add(new StreamRecord<>(null, 1200000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1501000, "Berlin", false), 1501000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1531000, "Poland", false), 1531000));
        add(new StreamRecord<>(null, 1500000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1601000, "Berlin", false), 1601000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1001000, "Berlin", false), 1001000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1561000, "Poland", false), 1561000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1551000, "Berlin", false), 1551000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611000, "Berlin", false), 1611000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 1900000, "Poland", true), 1900000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 1900000, "Berlin", true), 1900000));
        add(new StreamRecord<>(null, 1900000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1611000,  "Berlin", false), 1611000));
        add(new StreamRecord<>(new MeasurementOwn(100000000.0F, 2.0F, 0, 0, 2001000, "Berlin", false), 2001000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000000, "Poland", false), 2000000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2100000, "Poland", false), 2100000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2030000, "Poland", false), 2030000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2110000, "Poland", false), 2110000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1999000, "Poland", false), 1999000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2000000, "Poland", false), 2000000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 2200000, "Poland", true), 2200000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 2200000, "Berlin", true), 2200000));
        add(new StreamRecord<>(null, 2200000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 2201000, "Berlin", false), 2201000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 2900000, "Berlin", true), 2900000));
        add(new StreamRecord<>(null, 2900000));
    }};

    // !attention! only one watermark for last key is emitted for all partitions, in this case we have only one partition
    protected ArrayList<StreamRecord<AQIValue24h>> groundTruth = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 300000, true, "Berlin"), 300000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 300000, true, "Poland"), 300000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 600000, true, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 600000, true, "Berlin"), 600000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 900000, false, "Berlin"), 900000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 950000, true, "Berlin"), 950000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800000, false, "Poland"), 1800000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1900000, true, "Poland"), 1900000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1200000, false, "Berlin"), 1200000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1500000, false, "Berlin"), 1500000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1800000, false, "Berlin"), 1800000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 1900000, true, "Berlin"), 1900000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 2100000, false, "Poland"), 2100000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 1), 2200000, true, "Poland"), 2200000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 100000000), 2100000, false, "Berlin"), 2100000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 100000000), 2200000, true, "Berlin"), 2200000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 100000000), 2400000, false, "Berlin"), 2400000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 100000000), 2700000, false, "Berlin"), 2700000));
        add(new StreamRecord<>(new AQIValue24h(AQICalculator.getAQI(2, 100000000), 2900000, true, "Berlin"), 2900000));
    }};

    private static class EventKeySelector implements KeySelector<MeasurementOwn, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(MeasurementOwn e) {
            return e.getCity();
        }
    }

    @Test
    public void multipleCitiesAndSingleHughWindowTest() throws Exception {
        AQIValue24hProcessOperatorTestClass operator = new AQIValue24hProcessOperatorTestClass(0);

        KeyedOneInputStreamOperatorTestHarness<String, MeasurementOwn, AQIValue24h> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (StreamRecord<MeasurementOwn> e : events) {
                if (e.getValue() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTimestamp()));
                } else { // otherwise emit event
                    testHarness.processElement(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e.toString(), "");
        }

        int i = 0;

        for (Object item : testHarness.getOutput()) {
            if (item.getClass() == StreamRecord.class) {
                assertEquals(groundTruth.get(i), item);
                i++;
            }
        }

        testHarness.close();
    }

    //next test
    protected static ArrayList<StreamRecord<MeasurementOwn>> events2 = new ArrayList<>() {{
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 3000, "Poland", false), 3000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 250000, "Poland", false), 250000));
        add(new StreamRecord<>(new MeasurementOwn(1000.0F, 2.0F, 0, 0, 350000, "Poland", false), 350000));
        add(new StreamRecord<>(new MeasurementOwn(1000.0F, 2.0F, 0, 0, 650000, "Poland", false), 650000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1100000, "Poland", false), 1100000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1400000, "Poland", false), 1400000));
        add(new StreamRecord<>(new MeasurementOwn(1.0F, 2.0F, 0, 0, 1700000, "Poland", false), 1700000));
        add(new StreamRecord<>(new MeasurementOwn(0.0F, 0.0F, 0, 0, 1750000, "Poland", true), 1750000));
        add(new StreamRecord<>(null, 1750000)); // watermarks have data of null
    }};

    public static StreamRecord<AQIValue24h> correct = new StreamRecord<>(new AQIValue24h(
            -1,
            -1,
            false,
            "correct"),
            -1
    );

    public static class AQIValue24hProcessOperatorTestClass2 extends AQIValue24hProcessOperator {

        ArrayList<Slice> slicesPreWatermark = new ArrayList<>() {{
            add((new Slice(start, start + step) {{
                add(1.0F, 3000);
                add(1.0F, 250000);
            }}));
            add((new Slice(start + step, start + 2 * step) {{
                add(1000.0F, 350000);
            }}));
            add((new Slice(start + 2 * step, start + 3 * step) {{
                add(1000.0F, 650000);
            }}));
            add((new Slice(start + 3 * step, start + 4 * step) {{
                add(1.0F, 1100000);
            }}));
            add((new Slice(start + 4 * step, start + 5 * step) {{
                add(1.0F, 1400000);
            }}));
            add((new Slice(start + 5 * step, start + 6 * step) {{
                add(1.0F, 1700000);
            }}));
        }};

        ArrayList<Slice> slicesPostWatermark = new ArrayList<>() {{
            add((new Slice(start + 2 * step, start + 3 * step) {{
                add(1000.0F, 650000);
                addToWindow(1002.0F, 3);
            }}));
            add((new Slice(start + 3 * step, start + 4 * step) {{
                add(1.0F, 1100000);
                addToWindow(2000.0F, 2);
            }}));
            add((new Slice(start + 4 * step, start + 5 * step) {{
                add(1.0F, 1400000);
                addToWindow(1001.0F, 2);
            }}));
            add((new Slice(start + 5 * step, start + 6 * step) {{
                add(1.0F, 1700000);
            }}));
        }};

        public AQIValue24hProcessOperatorTestClass2(long start, long size, long step) {
            super(start, size, step);
        }

        @Override
        public void processElement(StreamRecord<MeasurementOwn> event) throws Exception {
            if (event.getValue().isWatermark()){
                if (state == null || state.value() == null) {
                    assertNull(slicesPreWatermark);
                }
                else {
                    assertEquals(slicesPreWatermark.size(), state.value().getSlicesNr());

                    for (int i = 0; i < slicesPreWatermark.size(); i++) {
                        Slice ss = state.value().getP1Slice(i);
                        Slice ts = slicesPreWatermark.get(i);

                        assertEquals(ts.getWindowSum(), ss.getWindowSum());
                        assertEquals(ts.getWindowCount(), ss.getWindowCount());
                        assertEquals(ts.getSum(), ss.getSum());
                        assertEquals(ts.getCount(), ss.getCount());

                        for (int j = 0; j < ss.getCount(); j++) {
                            Event se = ss.getEvents().get(j);
                            Event te = ts.getEvents().get(j);

                            assertEquals(te.getValue(), se.getValue());
                            assertEquals(te.getTimestamp(), se.getTimestamp());
                        }
                    }
                }
            }

            super.processElement(event);

            if (event.getValue().isWatermark()){
                if (state == null || state.value() == null) {
                    assertNull(slicesPostWatermark);
                }
                else {
                    assertEquals(slicesPostWatermark.size(), state.value().getSlicesNr());

                    for (int i = 0; i < slicesPostWatermark.size(); i++) {
                        Slice ss = state.value().getP1Slice(i);
                        Slice ts = slicesPostWatermark.get(i);

                        assertEquals(ts.getWindowSum(), ss.getWindowSum());
                        assertEquals(ts.getWindowCount(), ss.getWindowCount());
                        assertEquals(ts.getSum(), ss.getSum());
                        assertEquals(ts.getCount(), ss.getCount());

                        for (int j = 0; j < ss.getCount(); j++) {
                            Event se = ss.getEvents().get(j);
                            Event te = ts.getEvents().get(j);

                            assertEquals(te.getValue(), se.getValue());
                            assertEquals(te.getTimestamp(), se.getTimestamp());
                        }
                    }
                }

                super.output.collect(correct);
            }
        }
    }

    @Test
    public void consistentStateTest() throws Exception {

        AQIValue24hProcessOperatorTestClass2 operator = new AQIValue24hProcessOperatorTestClass2(0L, 900000L, 300000L);

        KeyedOneInputStreamOperatorTestHarness<String, MeasurementOwn, AQIValue24h> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (StreamRecord<MeasurementOwn> e : events2) {
                if (e.getValue() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTimestamp()));
                } else { // otherwise emit event
                    testHarness.processElement(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e.toString(), "");
        }

        int size = 0;
        Object last = null;

        for (Object item : testHarness.getOutput()) {
            ++size;
            if (item.getClass() == StreamRecord.class) {
                last = item;
            }
        }

        assertEquals(operator.slicesPreWatermark.size() + 2, size);
        assertEquals(last, correct);

        testHarness.close();
    }
}
