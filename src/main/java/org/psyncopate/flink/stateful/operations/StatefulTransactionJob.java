package org.psyncopate.flink.stateful.operations;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.psyncopate.flink.stateful.operations.pojo.KafkaMessage;
import org.psyncopate.flink.stateful.operations.pojo.UserPurchase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class StatefulTransactionJob {

    private Properties loadProperties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                throw new IOException("Sorry, unable to find " + fileName);
            }
            properties.load(input);
        }
        return properties;
    }

    public void execute() throws Exception {
        // Load properties
        Properties consumerProps = loadProperties("kafka-consumer-stateful-operations.properties");
        Properties producerProps = loadProperties("kafka-producer-stateful-operations.properties");

        // Configuration
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<KafkaMessage> kafkaSource = KafkaSource.<KafkaMessage>builder()
                .setBootstrapServers(consumerProps.getProperty("bootstrap.servers"))
                .setTopics(consumerProps.getProperty("topic"))
                .setGroupId(consumerProps.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(KafkaMessage.class))
                .build();

        // Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(producerProps.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(producerProps.getProperty("topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())  // Use StringSchema for JSON strings
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<KafkaMessage> stream = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        // Debug: Print incoming records
        stream.print("Kafka Source");

        // Extract payload and perform tumbling window operation
        KeyedStream<KafkaMessage.Transaction, String> keyedStream = stream
                .map(message -> {
                    System.out.println("Extracted payload: " + message.getPayload());  // Debug
                    return message.getPayload();
                })
                .keyBy(KafkaMessage.Transaction::getUser_id);

        DataStream<UserPurchase> processedStream = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountAggregateFunction(),new TotalPurchaseWindowFunction());

        // Debug: Print processed records before sending to Kafka
        processedStream.print("Processed Records");


        processedStream
                .map(userPurchase -> {
                    String json = userPurchase.toJson();
                    System.out.println("Sending to Kafka: " + json);  // Debug
                    return json;
                })
                .sinkTo(kafkaSink);

        env.execute("Stateful Transaction Job");
    }

    public static class CountAggregateFunction implements AggregateFunction<KafkaMessage.Transaction, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(KafkaMessage.Transaction value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class TotalPurchaseWindowFunction implements WindowFunction<Integer, UserPurchase, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<UserPurchase> out) throws Exception {
            int totalPurchases = input.iterator().next();
            UserPurchase userPurchase = new UserPurchase(key, totalPurchases, window.getStart(), window.getEnd());
            System.out.println("Window processed: " + userPurchase);  // Debug
            out.collect(userPurchase);
        }
    }

    public static void main(String[] args) throws Exception {
        StatefulTransactionJob job = new StatefulTransactionJob();
        job.execute();
    }
}
