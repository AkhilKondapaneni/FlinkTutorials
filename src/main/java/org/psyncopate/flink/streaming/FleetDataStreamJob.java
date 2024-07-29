package org.psyncopate.flink.streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.psyncopate.flink.streaming.pojo.FleetSensorData;
import org.apache.flink.formats.json.JsonSerializationSchema;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FleetDataStreamJob {

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
        Properties consumerProps = loadProperties("kafka-consumer.properties");
        Properties producerProps = loadProperties("kafka-producer-streaming.properties");

        // Configuration
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<FleetSensorData> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(consumerProps.getProperty("bootstrap.servers"))
                .setTopics(consumerProps.getProperty("topic"))
                .setGroupId(consumerProps.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema(FleetSensorData.class))
                .build();

        DataStream<FleetSensorData> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");



        // Processing logic (transform and filter as needed)
        DataStream<String> processedStream = stream
                .map(value -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

                    // Extract payload
                    FleetSensorData.Payload payload = value.getPayload();

                    // Convert payload to JSON string
                    return objectMapper.writeValueAsString(payload);
                });

        // Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(producerProps.getProperty("bootstrap.servers"))
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(producerProps.getProperty("topic"))
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        processedStream.sinkTo(kafkaSink);

        env.execute("Fleet Data Stream Job");
    }

    public static void main(String[] args) throws Exception {
        FleetDataStreamJob job = new FleetDataStreamJob();
        job.execute();
    }
}
