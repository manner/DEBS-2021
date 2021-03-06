package de.hpi.debs;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class AverageAQIAggregate implements AggregateFunction<MeasurementOwn, Tuple3<Float, Float, Integer>, Integer> {

    @Override
    public Tuple3<Float, Float, Integer> createAccumulator() {
        return Tuple3.of(0f, 0f, 0);
    }

    @Override
    public Tuple3<Float, Float, Integer> add(MeasurementOwn value, Tuple3<Float, Float, Integer> accumulator) {
        float sum1 = accumulator.f0 + value.getP1();
        float sum2 = accumulator.f1 + value.getP2();
        return Tuple3.of(sum1, sum2, accumulator.f2 + 1);
    }

    @Override
    public Integer getResult(Tuple3<Float, Float, Integer> accumulator) {
        float pm25Average = accumulator.f0 / accumulator.f2;
        float pm10Average = accumulator.f1 / accumulator.f2;
        return AQI.getAQI(pm25Average, pm10Average);
    }

    @Override
    public Tuple3<Float, Float, Integer> merge(Tuple3<Float, Float, Integer> a, Tuple3<Float, Float, Integer> b) {
        return Tuple3.of(a.f0 + b.f0, a.f1 + b.f1, a.f2 + b.f2);
    }
}
