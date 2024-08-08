# Fleet Data Stream Job

This project demonstrates an Apache Flink job for streaming processing. The job reads fleet sensor data from a Kafka topic, processes it, and writes the results back to another Kafka topic.

## Overview

The `FleetDataStreamJob` class showcases how to:
- Set up a Flink `StreamExecutionEnvironment` for streaming.
- Read data from a Kafka source.
- Apply transformations to the streaming data.
- Write the processed data to a Kafka sink.

## Features

- **Streaming Processing**: Configures Flink to run in streaming mode.
- **Kafka Source**: Reads fleet sensor data from a Kafka topic.
- **JSON Deserialization**: Deserializes incoming JSON messages into `FleetSensorData` objects.
- **Data Transformation**: Extracts payload from `FleetSensorData` and converts it back to JSON.
- **Kafka Sink**: Sends processed data to another Kafka topic.

## Requirements

- Apache Flink 
- Confluent Kafka
- Java 11
- Maven


## Configure Kafka
Create kafka-consumer.properties and kafka-producer-streaming.properties files in the src/main/resources directory with the necessary Kafka configurations.

## Build the jar
```bash
mvn clean package
```
## Running the Job
Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.streaming.FleetDataStreamJob target/flink-tutorial-1.1-<version>.jar
```

