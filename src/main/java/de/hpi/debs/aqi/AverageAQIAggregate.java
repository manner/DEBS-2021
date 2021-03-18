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
        float sumPM10;
        float sumPM25;
        int count;

        Aggregate() {
            sumPM10 = 0;
            sumPM25 = 0;
            count = 0;
        }

        public float getSumAQI10() {
            return sumPM10;
        }

        public float getSumAQI25() {
            return sumPM25;
        }

        public int getCount() {
            return count;
        }

        public Aggregate merge(Aggregate a) {
            sumPM10 += a.getSumAQI10();
            sumPM25 += a.getSumAQI25();
            count += a.getCount();
            return this;
        }

        public Aggregate add(MeasurementOwn m) {
            sumPM10 += m.getP1();
            sumPM25 += m.getP2();
            count++;
            return this;
        }

        public Integer getAQI() {
            float pm25Average = sumPM25 / count;
            float pm10Average = sumPM10 / count;
            return AQICalculator.getAQI(pm25Average, pm10Average);
        }
    }
}
