package de.hpi.debs;

import de.hpi.debs.aqi.*;
import de.hpi.debs.slicing.AqiSlice;
import de.hpi.debs.slicing.Slice;
import de.hpi.debs.testHarness.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

public class AQIValue5dProcessOperatorTests {

    public static class AQIValue5dProcessOperatorTestClass extends AQIValue5dProcessOperator {

        public AQIValue5dProcessOperatorTestClass(long start) {
            super(start);
        }

        public AQIValue5dProcessOperatorTestClass(long start, long size, long step) {
            super(start, size, step);
        }
    }

    private static class EventKeySelector implements KeySelector<AQIValue24h, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(AQIValue24h e) {
            return e.getCity();
        }
    }

    protected ArrayList<StreamRecord<AQIValue24h>> events = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 8000, true, "Poland"), 8000));
        add(new StreamRecord<>(null, 8000)); // watermarks have data of null
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 16000, true, "Berlin"), 16000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 16000, true, "Poland"), 16000));
        add(new StreamRecord<>(null, 16000)); // watermarks have data of null
        add(new StreamRecord<>(new AQIValue24h(0, 4, 3, 4, 24000, true, "Poland"), 24000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 5, 24000, true, "Berlin"), 24000));
        add(new StreamRecord<>(null, 24000)); // watermarks have data of null
        add(new StreamRecord<>(new AQIValue24h(0, 4, 2, 4, 36000, true, "Berlin"), 36000));
        add(new StreamRecord<>(null, 36000));
    }};

    // !attention! only one watermark for last key is emitted for all partitions, in this case we have only one partition
    protected ArrayList<StreamRecord<AQIValue5d>> groundTruth = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 8000, true, "Poland"), 8000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 16000, true, "Berlin"), 16000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 16000, true, "Poland"), 16000));
        add(new StreamRecord<>(new AQIValue5d(0, 4.0, 3, 4, 24000, true, "Poland"), 24000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 5, 24000, true, "Berlin"), 24000));
        add(new StreamRecord<>(new AQIValue5d(0, 4.0, 2, 4, 36000, true, "Berlin"), 36000));
    }};

    @Test
    public void initialManyBatchesInFirst5MinutesTest() throws Exception {
        AQIValue5dProcessOperatorTestClass operator = new AQIValue5dProcessOperatorTestClass(0);

        KeyedOneInputStreamOperatorTestHarness<String, AQIValue24h, AQIValue5d> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (StreamRecord<AQIValue24h> e : events) {
                if (e.getValue() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTimestamp()));
                } else { // otherwise emit event
                    testHarness.processElement(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e, "");
        }

        int i = 0;

        assertEquals(events.size(), testHarness.getOutput().size(), "Output has not correct number of events.");

        for (Object item : testHarness.getOutput()) {
            if (item.getClass() == StreamRecord.class) {
                assertEquals(groundTruth.get(i), item);
                i++;
            }
        }

        testHarness.close();
    }

    // next test
    protected ArrayList<StreamRecord<AQIValue24h>> events2 = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 8000, true, "Poland"), 8000));
        add(new StreamRecord<>(null, 8000)); // watermarks have data of null
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 16000, true, "Berlin"), 16000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 16000, true, "Poland"), 16000));
        add(new StreamRecord<>(null, 16000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 24000, true, "Poland"), 24000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 5, 24000, true, "Berlin"), 24000));
        add(new StreamRecord<>(null, 24000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 36000, true, "Berlin"), 36000));
        add(new StreamRecord<>(null, 36000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 300000, false, "Poland"), 300000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 300000, false, "Berlin"), 300000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 600000, false, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 4, 600000, false, "Berlin"), 600000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 600000, true, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue24h(0, 2, 2, 5, 600000, true, "Berlin"), 600000));
        add(new StreamRecord<>(null, 600000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 900000, false, "Poland"), 900000));
        add(new StreamRecord<>(new AQIValue24h(0, 5, 2, 4, 900000, false, "Berlin"), 900000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 950000, true, "Poland"), 950000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 5, 950000, true, "Berlin"), 950000));
        add(new StreamRecord<>(null, 950000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 1200000, false, "Poland"), 1200000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 1200000, false, "Berlin"), 1200000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 1250000, true, "Poland"), 1250000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 5, 1250000, true, "Berlin"), 1250000));
        add(new StreamRecord<>(null, 1250000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 1500000, false, "Poland"), 1500000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 4, 1500000, false, "Berlin"), 1500000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 3, 4, 1850000, true, "Poland"), 1850000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 5, 1850000, true, "Berlin"), 1850000));
        add(new StreamRecord<>(null, 1850000));
    }};

    // !attention! only one watermark for last key is emitted for all partitions, in this case we have only one partition
    protected ArrayList<StreamRecord<AQIValue5d>> groundTruth2 = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 8000, true, "Poland"), 8000));
        add(new StreamRecord<>(null, 8000)); // watermarks have data of null
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 16000, true, "Berlin"), 16000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 16000, true, "Poland"), 16000));
        add(new StreamRecord<>(null, 16000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 24000, true, "Poland"), 24000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 5, 24000, true, "Berlin"), 24000));
        add(new StreamRecord<>(null, 24000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 36000, true, "Berlin"), 36000));
        add(new StreamRecord<>(null, 36000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 300000, false, "Poland"), 300000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 2, 4, 300000, false, "Berlin"), 300000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 600000, false, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue5d(0, 2.0, 2, 4, 600000, false, "Berlin"), 600000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 600000, true, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue5d(0, 2.0, 2, 5, 600000, true, "Berlin"), 600000));
        add(new StreamRecord<>(null, 600000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 900000, false, "Poland"), 900000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 4, 900000, false, "Berlin"), 900000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 950000, true, "Poland"), 950000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 5, 950000, true, "Berlin"), 950000));
        add(new StreamRecord<>(null, 950000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 1200000, false, "Poland"), 1200000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 4, 1200000, false, "Berlin"), 1200000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 1250000, true, "Poland"), 1250000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 5, 1250000, true, "Berlin"), 1250000));
        add(new StreamRecord<>(null, 1250000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 1500000, false, "Poland"), 1500000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 4, 1500000, false, "Berlin"), 1500000));
        add(new StreamRecord<>(new AQIValue5d(0, 1.0, 3, 4, 1850000, true, "Poland"), 1850000));
        add(new StreamRecord<>(new AQIValue5d(0, 3.0, 2, 5, 1850000, true, "Berlin"), 1850000));
        add(new StreamRecord<>(null, 1850000));
    }};

    @Test
    public void multipleBatchesTest() throws Exception {
        AQIValue5dProcessOperatorTestClass operator = new AQIValue5dProcessOperatorTestClass(0, 900000, 300000);

        KeyedOneInputStreamOperatorTestHarness<String, AQIValue24h, AQIValue5d> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (StreamRecord<AQIValue24h> e : events2) {
                if (e.getValue() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTimestamp()));
                } else { // otherwise emit event
                    testHarness.processElement(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e, "");
        }

        assertEquals(events2.size(), testHarness.getOutput().size(), "Output has not correct number of events.");

        for (Object item : testHarness.getOutput()) {
            if (item.getClass() == StreamRecord.class) // skip watermarks as flink should process them correctly
                assertTrue(groundTruth2.contains(item), "Item is not in ground truth:\n" + item + "\n");
        }

        testHarness.close();
    }

    //next test
    protected static ArrayList<StreamRecord<AQIValue24h>> events3 = new ArrayList<>() {{
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 3000, true, "Poland"), 3000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 250000, true, "Poland"), 250000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 4, 300000, false, "Poland"), 300000));
        add(new StreamRecord<>(new AQIValue24h(0, 3, 2, 5, 600000, false, "Poland"), 600000));
        add(new StreamRecord<>(new AQIValue24h(0, 5, 2, 6, 900000, false, "Poland"), 900000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 1, 7, 1200000, false, "Poland"), 1200000));
        add(new StreamRecord<>(new AQIValue24h(0, 6, 2, 8, 1500000, false, "Poland"), 1500000));
        add(new StreamRecord<>(new AQIValue24h(0, 1, 2, 9, 1750000, true, "Poland"), 1750000));
        add(new StreamRecord<>(null, 1750000)); // watermarks have data of null
    }};

    public static StreamRecord<AQIValue5d> correct = new StreamRecord<>(new AQIValue5d(
            0,
            -1,
            -1,
            -1,
            -1,
            false,
            "correct"),
            -1
    );

    public static class AQIValue5dProcessOperatorTestClass2 extends AQIValue5dProcessOperator {

        ArrayList<AqiSlice> slicesPreWatermark = new ArrayList<>() {{
            add((new AqiSlice(start, start + step) {{
                add(1.0, 2, 4, 300000);
            }}));
            add((new AqiSlice(start + step, start + 2 * step) {{
                add(3.0, 2, 5, 600000);
            }}));
            add((new AqiSlice(start + 2 * step, start + 3 * step) {{
                add(5.0, 2, 6, 900000);
            }}));
            add((new AqiSlice(start + 3 * step, start + 4 * step) {{
                add(1.0, 1, 7, 1200000);
            }}));
            add((new AqiSlice(start + 4 * step, start + 5 * step) {{
                add(6.0, 2, 8, 1500000);
            }}));
        }};

        ArrayList<AqiSlice> slicesPostWatermark = new ArrayList<>() {{
            add((new AqiSlice(start + 2 * step, start + 3 * step) {{
                add(5.0, 2, 6, 900000);
                addToWindow(4.0, 2);
            }}));
            add((new AqiSlice(start + 3 * step, start + 4 * step) {{
                add(1.0, 1, 7, 1200000);
                addToWindow(8.0, 2);
            }}));
            add((new AqiSlice(start + 4 * step, start + 5 * step) {{
                add(6.0, 2, 8, 1500000);
                addToWindow(6.0, 2);
            }}));
            add((new AqiSlice(start + 5 * step, start + 6 * step)));
        }};

        public AQIValue5dProcessOperatorTestClass2(long start, long size, long step) {
            super(start, size, step);
        }

        @Override
        public void processElement(StreamRecord<AQIValue24h> event) throws Exception {
            if (event.getValue().isWatermark() && event.getTimestamp() == 1750000L){
                if (state == null || state.value() == null) {
                    assertNull(slicesPreWatermark);
                }
                else {
                    assertEquals(slicesPreWatermark.size(), state.value().getSlicesNr());

                    for (int i = 0; i < slicesPreWatermark.size(); i++) {
                        Slice ss = state.value().getAqiSlice(i);
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

            if (event.getValue().isWatermark() && event.getTimestamp() == 1750000L){
                if (state == null || state.value() == null) {
                    assertNull(slicesPostWatermark);
                }
                else {
                    assertEquals(slicesPostWatermark.size(), state.value().getSlicesNr());

                    for (int i = 0; i < slicesPostWatermark.size(); i++) {
                        Slice ss = state.value().getAqiSlice(i);
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

        AQIValue5dProcessOperatorTestClass2 operator = new AQIValue5dProcessOperatorTestClass2(0L, 900000L, 300000L);

        KeyedOneInputStreamOperatorTestHarness<String, AQIValue24h, AQIValue5d> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new EventKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO
                );

        testHarness.open();

        try {
            for (StreamRecord<AQIValue24h> e : events3) {
                if (e.getValue() == null) { // emit watermark
                    testHarness.processWatermark(new Watermark(e.getTimestamp()));
                } else { // otherwise emit event
                    testHarness.processElement(e);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals("testRun failed with error message: " + e, "");
        }

        int size = 0;
        Object last = null;

        for (Object item : testHarness.getOutput()) {
            ++size;
            if (item.getClass() == StreamRecord.class) {
                last = item;
            }
        }

        assertEquals(events3.size() + 1, size); // + 1 for correct event
        assertEquals(last, correct);

        testHarness.close();
    }
}
