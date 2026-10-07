/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.util.stream.Collectors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class CalculateAverage_baseline {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, int value) {
        private Measurement(String[] parts) {
            this(parts[0], parseTenths(parts[1]));
        }

        private static int parseTenths(String value) {
            boolean negative = value.charAt(0) == '-';
            int start = negative ? 1 : 0;
            int decimal = value.indexOf('.');
            int result = Integer.parseInt(value.substring(start, decimal)) * 10 + value.charAt(decimal + 1) - '0';
            return negative ? -result : result;
        }
    }

    private static record ResultRow(int min, long mean, int max) {

        public String toString() {
            return format(min) + "/" + format(mean) + "/" + format(max);
        }

        private String format(long value) {
            boolean negative = value < 0;
            long absolute = Math.abs(value);
            return (negative ? "-" : "") + absolute / 10 + "." + absolute % 10;
        }
    };

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private long count;
    }

    private static long roundedMean(long sum, long count) {
        long quotient = sum / count;
        long remainder = sum % count;
        if (remainder < 0) {
            quotient--;
            remainder += count;
        }
        return quotient + (remainder * 2 >= count ? 1 : 0);
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    var res = new MeasurementAggregator();
                    res.min = Math.min(agg1.min, agg2.min);
                    res.max = Math.max(agg1.max, agg2.max);
                    res.sum = agg1.sum + agg2.sum;
                    res.count = agg1.count + agg2.count;

                    return res;
                },
                agg -> {
                    return new ResultRow(agg.min, roundedMean(agg.sum, agg.count), agg.max);
                });

        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
                .map(l -> new Measurement(l.split(";")))
                .collect(groupingBy(m -> m.station(), collector)));

        System.out.println(measurements);
    }
}
