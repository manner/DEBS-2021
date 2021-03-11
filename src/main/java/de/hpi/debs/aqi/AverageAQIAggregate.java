package de.hpi.debs.aqi;

import org.apache.flink.api.common.functions.AggregateFunction;

import de.hpi.debs.MeasurementOwn;

public class AverageAQIAggregate implements AggregateFunction<MeasurementOwn, AverageAQIAggregate.Aggregate, Integer> {

    @Override
    public Aggregate createAccumulator() {
        return new Aggregate();
    }

    @Override
    public Aggregate add(MeasurementOwn value, Aggregate accumulator) {
        return accumulator.add(value);
    }

    @Override
    public Integer getResult(Aggregate accumulator) {
        return accumulator.getAQI();
    }

    @Override
    public Aggregate merge(Aggregate a, Aggregate b) {
        return a.merge(b);
    }

    public static class Aggregate {
        float sumAQI10;
        float sumAQI25;
        int count;

        Aggregate() {
            sumAQI10 = 0;
            sumAQI25 = 0;
            count = 0;
        }

        public float getSumAQI10() {
            return sumAQI10;
        }

        public float getSumAQI25() {
            return sumAQI25;
        }

        public int getCount() {
            return count;
        }

        public Aggregate merge(Aggregate a) {
            sumAQI10 += a.getSumAQI10();
            sumAQI25 += a.getSumAQI25();
            count += a.getCount();
            return this;
        }

        public Aggregate add(MeasurementOwn m) {
            sumAQI10 += m.getP1();
            sumAQI25 += m.getP2();
            count++;
            return this;
        }

        public Integer getAQI() {
            float pm25Average = sumAQI25 / count;
            float pm10Average = sumAQI10 / count;
            return AQICalculator.getAQI(pm25Average, pm10Average);
        }
    }
}
