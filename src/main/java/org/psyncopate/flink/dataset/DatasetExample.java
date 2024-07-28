package org.psyncopate.flink.dataset;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasetExample {

    private static final Logger LOG = LoggerFactory.getLogger(DatasetExample.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // Load Kafka producer properties from an external file
        Properties kafkaProducerProps = new Properties();
        try (InputStream stream = DatasetExample.class.getClassLoader().getResourceAsStream("kafka-producer.properties")) {
            kafkaProducerProps.load(stream);
        }

        // Example data stream
        DataStream<String> text = env.fromElements("Flink", "DataStream", "API", "Example");

        // Apply a simple transformation
        DataStream<String> result = text.map(value -> "Hello, " + value);

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
}
