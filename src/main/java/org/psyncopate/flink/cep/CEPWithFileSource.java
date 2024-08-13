package org.psyncopate.flink.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class CEPWithFileSource {

    public static void main(String[] args) throws Exception {
        // Set up the local execution environment
        final LocalStreamEnvironment env = LocalStreamEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        // Read data from CSV file, skipping the header row
        DataStream<String> inputDataStream = env.readTextFile("src/main/resources/cep_transactions.csv")
                .filter(line -> !line.startsWith("user_id"));

        // Parse the CSV data
        DataStream<Tuple4<String, Integer, LocalDateTime, Double>> transactions = inputDataStream
                .map(new MapFunction<String, Tuple4<String, Integer, LocalDateTime, Double>>() {
                    @Override
                    public Tuple4<String, Integer, LocalDateTime, Double> map(String line) {
                        String[] fields = line.split(",");
                        String userId = fields[0];
                        int transactionId = Integer.parseInt(fields[1]);
                        LocalDateTime timestamp = LocalDateTime.parse(fields[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                        double amount = Double.parseDouble(fields[3]);
                        return new Tuple4<>(userId, transactionId, timestamp, amount);
                    }
                });

        // Assign timestamps and watermarks
        DataStream<Tuple4<String, Integer, LocalDateTime, Double>> timestampedTransactions = transactions
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, Integer, LocalDateTime, Double>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.f2.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()));

        // Define the CEP pattern: Detect users with high consecutive transactions within a window
        Pattern<Tuple4<String, Integer, LocalDateTime, Double>, ?> highTransactionPattern = Pattern.<Tuple4<String, Integer, LocalDateTime, Double>>begin("start")
                .where(new SimpleCondition<Tuple4<String, Integer, LocalDateTime, Double>>() {
                    @Override
                    public boolean filter(Tuple4<String, Integer, LocalDateTime, Double> transaction) {
                        return transaction.f3 > 200.00;
                    }
                })
                .next("next")
                .where(new IterativeCondition<Tuple4<String, Integer, LocalDateTime, Double>>() {
                    @Override
                    public boolean filter(Tuple4<String, Integer, LocalDateTime, Double> transaction, Context<Tuple4<String, Integer, LocalDateTime, Double>> context) throws Exception {
                        for (Tuple4<String, Integer, LocalDateTime, Double> prev : context.getEventsForPattern("start")) {
                            if (transaction.f0.equals(prev.f0)) {
                                return transaction.f3 > 200.00;
                            }
                        }
                        return false;
                    }
                })
                .within(Time.minutes(10));

        // Apply the pattern on the input stream
        DataStream<String> highTransactionAlerts = CEP.pattern(timestampedTransactions.keyBy(value -> value.f0), highTransactionPattern)
                .select(new PatternSelectFunction<Tuple4<String, Integer, LocalDateTime, Double>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple4<String, Integer, LocalDateTime, Double>>> pattern) {
                        Tuple4<String, Integer, LocalDateTime, Double> first = pattern.get("start").get(0);
                        Tuple4<String, Integer, LocalDateTime, Double> second = pattern.get("next").get(0);
                        return "User " + first.f0 + " had two high transactions: " + first.f3 + " and " + second.f3 + " within 10 minutes.";
                    }
                });

        // Print the results
        highTransactionAlerts.print();


        // Execute the job
        env.execute("CEP with Externalized Source Data and Transformations");
    }
}
