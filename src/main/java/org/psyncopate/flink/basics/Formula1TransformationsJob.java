package org.psyncopate.flink.basics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Formula1TransformationsJob {

    public static void main(String[] args) throws Exception {
        // Set up the local execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Read data from CSV file using TextInputFormat for streaming
        DataStream<String> inputDataStream = env.readTextFile("src/main/resources/f1_lap_times.csv");

        // Parse the CSV data with error handling
        DataStream<Tuple4<Integer, String, String, Double>> source = inputDataStream
                .flatMap(new FlatMapFunction<String, Tuple4<Integer, String, String, Double>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple4<Integer, String, String, Double>> out) {
                        // Skip header line if present
                        if (line.startsWith("driver_id")) return;

                        String[] fields = line.split(",");
                        if (fields.length == 4) {
                            try {
                                int driverId = Integer.parseInt(fields[0].trim());
                                String driverName = fields[1].trim();
                                String teamName = fields[2].trim();
                                double lapTime = Double.parseDouble(fields[3].trim());
                                out.collect(new Tuple4<>(driverId, driverName, teamName, lapTime));
                            } catch (NumberFormatException e) {
                                // Log or handle parsing errors
                                System.err.println("Skipping line due to parsing error: " + line);
                            }
                        }
                    }
                }).name("CSV Source");

        // Print parsed data
        source.print("Source");

        // Custom Transformation 1: Convert lap times to milliseconds
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Double>> mapped = source.map(new MapFunction<Tuple4<Integer, String, String, Double>, Tuple4<Integer, String, String, Double>>() {
            @Override
            public Tuple4<Integer, String, String, Double> map(Tuple4<Integer, String, String, Double> value) {
                // Convert seconds to milliseconds
                return new Tuple4<>(value.f0, value.f1, value.f2, value.f3 * 1000);
            }
        }).name("Convert to milliseconds");

        // Print after mapping
        mapped.print("Mapped");

        // Custom Transformation 2: Filter out slow laps (above 89 seconds)
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Double>> filtered = mapped.filter(new FilterFunction<Tuple4<Integer, String, String, Double>>() {
            @Override
            public boolean filter(Tuple4<Integer, String, String, Double> value) {
                // Filter lap times greater than 89 seconds (89,000 ms)
                return value.f3 < 89000;
            }
        }).name("Filter lap times under 89 seconds");

        // Print after filtering
        filtered.print("Filtered");

        // Custom Transformation 3: Enrich data with custom logic (e.g., flag fast laps)
        SingleOutputStreamOperator<Tuple4<Integer, String, String, String>> enriched = filtered.map(new MapFunction<Tuple4<Integer, String, String, Double>, Tuple4<Integer, String, String, String>>() {
            @Override
            public Tuple4<Integer, String, String, String> map(Tuple4<Integer, String, String, Double> value) {
                String speedCategory = value.f3 < 88000 ? "Fast Lap" : "Standard Lap";
                return new Tuple4<>(value.f0, value.f1, value.f2, speedCategory);
            }
        }).name("Categorize laps");

        // Print after enrichment
        enriched.print("Enriched");

        // Custom Transformation 4: Count laps per driver
        DataStream<Tuple2<String, Integer>> lapCounts = enriched.flatMap(new FlatMapFunction<Tuple4<Integer, String, String, String>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple4<Integer, String, String, String> value, Collector<Tuple2<String, Integer>> out) {
                // For each lap, output a count of 1 with the driver's name
                out.collect(new Tuple2<>(value.f1, 1));
            }
        }).name("Count laps");

        // Print lap counts
        lapCounts.print("Lap Counts");

        // Reduce Transformation: Sum the lap counts per driver
        SingleOutputStreamOperator<Tuple2<String, Integer>> totalLaps = lapCounts
                .keyBy(value -> value.f0) // Group by Driver Name
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
                        // Sum the lap counts
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                }).name("Sum lap counts per driver");

        // Print total laps per driver
        totalLaps.print("Total Laps");

        // Custom Transformation 5: Identify drivers with all "Fast Laps"
        SingleOutputStreamOperator<String> fastDrivers = enriched
                .keyBy(value -> value.f1)
                .reduce(new ReduceFunction<Tuple4<Integer, String, String, String>>() {
                    @Override
                    public Tuple4<Integer, String, String, String> reduce(Tuple4<Integer, String, String, String> value1, Tuple4<Integer, String, String, String> value2) {
                        // Check if both laps are "Fast Lap" and retain the driver name
                        if (!"Fast Lap".equals(value1.f3) || !"Fast Lap".equals(value2.f3)) {
                            value1.f3 = "Not All Fast Laps";
                        }
                        return value1;
                    }
                }).filter(value -> "Fast Lap".equals(value.f3))
                .map(value -> value.f1 + " had all fast laps!")
                .name("Identify drivers with all fast laps");

        // Print drivers with all fast laps
        fastDrivers.print("Fast Drivers");

        // Execute the job
        env.execute("Formula 1 Transformations");
    }
}
