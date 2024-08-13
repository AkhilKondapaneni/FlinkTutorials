
# StatefulTransactionJob

This project demonstrates an Apache Flink job for stateful stream processing. The job reads JSON data from a Kafka topic, processes it using tumbling windows, and writes the results to another Kafka topic.

## Overview

The `StatefulTransactionJob` class showcases how to:
- Set up a Flink `StreamExecutionEnvironment` for streaming mode.
- Read data from a Kafka topic using a Kafka source.
- Process the data using stateful operations, including tumbling windows and aggregation.
- Write the results to another Kafka topic using a Kafka sink.

## Features

- **Stateful Streaming**: Configures Flink for stateful stream processing with tumbling windows.
- **Kafka Source**: Reads data from a Kafka topic with deserialization.
- **Data Aggregation**: Counts transactions per user within each window.
- **Kafka Sink**: Sends aggregated data to another Kafka topic.

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

Create `kafka-consumer-stateful-operations.properties` and `kafka-producer-stateful-operations.properties` files in the `src/main/resources` directory with the following properties:

### kafka-consumer-stateful-operations.properties

```properties
bootstrap.servers=<your-kafka-bootstrap-servers>
topic=<your-input-kafka-topic>
group.id=<your-consumer-group-id>
```

### kafka-producer-stateful-operations.properties

```properties
bootstrap.servers=<your-kafka-bootstrap-servers>
topic=<your-output-kafka-topic>
```

Replace `<your-kafka-bootstrap-servers>`, `<your-input-kafka-topic>`, `<your-output-kafka-topic>`, and `<your-consumer-group-id>` with your Kafka brokers' addresses, input and output topics, and consumer group ID.

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.stateful.operations.StatefulTransactionJob target/flink-tutorial-1.1-<version>.jar
```

## Code Overview

**StatefulTransactionJob**: Main class to run the Flink job.

- **loadProperties method**: Loads Kafka properties from external files.
- **execute method**:
    - Configures Flink's streaming environment.
    - Sets up a Kafka source with deserialization of `KafkaMessage`.
    - Applies a tumbling window operation to aggregate transactions per user.
    - Configures a Kafka sink to write the aggregated data to a Kafka topic.
    - Executes the Flink job.

## Data Transformations Explained

### Aggregation with Tumbling Windows

**Operation: `keyedStream.window(TumblingProcessingTimeWindows.of(Time.minutes(1))).aggregate(new CountAggregateFunction(), new TotalPurchaseWindowFunction())`**

**Purpose:**
- **CountAggregateFunction**: Counts the number of transactions for each user within a one-minute window.
- **TotalPurchaseWindowFunction**: Collects the count of transactions and creates a `UserPurchase` object, which includes the user ID, total purchases, and window time.

### Kafka Sink

**Operation: `processedStream.map(userPurchase -> userPurchase.toJson()).sinkTo(kafkaSink)`**

**Purpose:** Converts the `UserPurchase` object to a JSON string and sends it to the output Kafka topic. This transformation ensures that the aggregated data is formatted as JSON and delivered to the target Kafka topic.

## Dependencies

- Apache Flink
- Apache Kafka
- Jackson JSON Processor

---
