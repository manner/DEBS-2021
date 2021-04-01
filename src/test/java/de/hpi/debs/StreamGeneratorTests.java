package de.hpi.debs;

import de.hpi.debs.slicing.Slice;
import de.tum.i13.bandency.Benchmark;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamGeneratorTests {

    public static class StreamGeneratorTestClass extends StreamGenerator {

        public StreamGeneratorTestClass(
                Benchmark benchmarkIn,
                int batchNumbersIn) {
            super(benchmarkIn, batchNumbersIn);
        }
    }

    protected ArrayList<Event> events = new ArrayList<>() {{
        add(new Event(10, 99));
        add(new Event(10, 200));
        add(new Event(10, 300));
        add(new Event(10, 611));
        add(new Event(10, 1501));
        add(new Event(10, 1601));
        add(new Event(10, 1001));
        add(new Event(10, 1551));
        add(new Event(10, 1611));
        add(new Event(10, 2001));
    }};

    @Test
    public void summingTest() {
    }
}
