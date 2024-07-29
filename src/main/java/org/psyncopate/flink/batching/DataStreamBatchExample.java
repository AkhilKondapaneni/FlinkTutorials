package org.psyncopate.flink.batching;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamBatchExample {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Load Kafka producer properties from an external file
        Properties kafkaProducerProps = new Properties();
        try (InputStream stream = DatasetExample.class.getClassLoader().getResourceAsStream("kafka-producer.properties")) {
            kafkaProducerProps.load(stream);
        }

        // Generate a larger dataset to increase load
        List<String> userData = IntStream.range(0, 10000).mapToObj(i -> generateUserData(i)).collect(Collectors.toList());
        DataStream<String> text = env.fromCollection(userData);

        // Apply transformations
        DataStream<String> result = text
                .map(value -> "Processed: " + value) // Simple transformation
                .filter(value -> value.contains("Active: true")) // Filtering active users
                .keyBy(value -> value.split(",")[0]) // Key by user ID
                .reduce((value1, value2) -> value1 + "; " + value2); // Aggregation

        // Create Kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaProducerProps.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setTopic(kafkaProducerProps.getProperty("topic"))
                        .build())
                .setKafkaProducerConfig(kafkaProducerProps)
                .build();

        // Add sink to data stream
        result.sinkTo(kafkaSink).name("Kafka Sink");

        // Execute the Flink job
        env.execute("DataStream to Kafka Example");
    }

    private static String generateUserData(int id) {
        Random random = new Random();
        String name = "user" + id;
        int age = random.nextInt(60) + 18;
        String email = name + "@example.com";
        boolean isActive = random.nextBoolean();
        return String.format("ID: %d, Name: %s, Age: %d, Email: %s, Active: %b", id, name, age, email, isActive);
    }
}
