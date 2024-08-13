
# FleetDataStreamJob

This project demonstrates a basic Apache Flink job for streaming data processing. The job reads JSON data from a Kafka topic, processes it, and writes the results to another Kafka topic.

## Overview

The `FleetDataStreamJob` class showcases how to:
- Set up a Flink `StreamExecutionEnvironment` for streaming mode.
- Read data from a Kafka topic using a Kafka source.
- Process the data by transforming it into a JSON string.
- Write the processed data to a Kafka topic using a Kafka sink.

## Features

- **Streaming Processing**: Configures Flink to run in streaming mode.
- **Kafka Source**: Reads data from a Kafka topic with deserialization.
- **Data Transformation**: Converts payloads into JSON strings.
- **Kafka Sink**: Sends processed data to another Kafka topic.

## Requirements

- Apache Flink (compatible with the version used in this project)
- Apache Kafka
- Java 11 or higher

## Setup

**Clone the Repository**

```bash
git clone <repository-url>
cd <repository-directory>
```

## Build

```bash
mvn clean package
```

## Configure Kafka

Create a datagen source connector that will pump data into a source topic. Use the following config for spinning up the connector

```json
{
  "name": "StreamingJob_Connector",
  "config": {
    "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
    "tasks.max": "2",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "kafka.topic": "<REPLACE_WITH_TOPIC>",
    "quickstart": "fleet_mgmt_sensors"
  }
}
```

Create `kafka-consumer-streaming.properties` and `kafka-producer-streaming.properties` files in the `src/main/resources` directory with the following properties:


### kafka-consumer.properties

```properties
bootstrap.servers=<your-kafka-bootstrap-servers>
topic=<your-input-kafka-topic>
group.id=<your-consumer-group-id>
```

### kafka-producer-streaming.properties

```properties
bootstrap.servers=<your-kafka-bootstrap-servers>
topic=<your-output-kafka-topic>
```

Replace `<your-kafka-bootstrap-servers>`, `<your-input-kafka-topic>`, `<your-output-kafka-topic>`, and `<your-consumer-group-id>` with your Kafka brokers' addresses, input and output topics, and consumer group ID.

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.streaming.FleetDataStreamJob target/flink-tutorial-1.1-<version>.jar
```

## Code Overview

**FleetDataStreamJob**: Main class to run the Flink job.

- **loadProperties method**: Loads Kafka properties from external files.
- **execute method**:
    - Configures Flink's streaming environment.
    - Sets up a Kafka source with deserialization of `FleetSensorData`.
    - Applies a transformation to convert the payload of `FleetSensorData` to a JSON string.
    - Configures a Kafka sink to write the processed data to a Kafka topic.
    - Executes the Flink job.

## Data Transformations Explained

### JSON Conversion

**Operation: `stream.map(value -> { ... })`**

**Purpose:** Converts the payload of `FleetSensorData` into a JSON string. This transformation prepares the data for storage or further processing by converting it into a consistent format.

### Kafka Sink

**Operation: `processedStream.sinkTo(kafkaSink)`**

**Purpose:** Sends the transformed data to the specified Kafka topic. The Kafka sink ensures that the processed data is delivered to the output Kafka topic with at least once delivery guarantee.

## Dependencies

- Apache Flink
- Apache Kafka
- Jackson JSON Processor

---

