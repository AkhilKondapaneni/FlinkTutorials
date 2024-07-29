# DataStreamBatchExample

This project demonstrates a basic Apache Flink job for batch processing. The job reads data from a local collection, processes it, and writes the results to a Kafka topic.

## Overview

The `DataStreamBatchExample` class showcases how to:
- Set up a Flink `StreamExecutionEnvironment` in batch mode.
- Generate a sample dataset for processing.
- Apply basic transformations to the dataset.
- Write the processed data to a Kafka topic using a Kafka sink.

## Features

- **Batch Processing**: Configures Flink to run in batch mode.
- **Data Generation**: Creates a sample dataset with 10,000 entries.
- **Transformations**: Applies map, filter, and reduce transformations.
- **Kafka Sink**: Sends processed data to a Kafka topic.

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

Create a kafka-producer.properties file in the src/main/resources directory with the following properties:
``` properties
bootstrap.servers=<your-kafka-bootstrap-servers>
topic=<your-kafka-topic>
```
Replace <your-kafka-bootstrap-servers> with your Kafka brokers' addresses and <your-kafka-topic> with the target Kafka topic

## Running the Job

Execute the Flink job using the following command:
```bash
flink run -c org.psyncopate.flink.batching.DataStreamBatchExample target/flink-tutorial-1.1-<version>.jar

```
## Code Overview

**DataStreamBatchExample**: Main class to run the Flink job.
main method:
Sets up the Flink execution environment in batch mode.

Loads Kafka producer properties from an external file.
Generates a sample dataset with 10,000 entries.

**Applies transformations:**

**Map Transformation:** Prepends "Processed: " to each entry.
**Filter Transformation**: Keeps only entries that contain "Active: true".
**KeyBy Transformation**: Groups entries by user ID (extracted from the entry).
**Reduce Transformation**: Aggregates entries by concatenating them with "; ".
Configures a Kafka sink and writes the processed data to a Kafka topic.

**generateUserData method**: Generates random user data for the dataset.

## Data Transformations Explained
### Map Transformation:

**Operation: text.map(value -> "Processed: " + value)**

**Purpose:** Adds the prefix "Processed: " to each data entry. This transformation is useful for tagging or annotating data.
Filter Transformation:

**Operation: result.filter(value -> value.contains("Active: true"))** 

**Purpose**: Filters out entries that do not contain "Active: true". This is used to focus on only those records that meet a specific condition (e.g., active users).
KeyBy Transformation:

**Operation: result.keyBy(value -> value.split(",")[0])**

**Purpose**: Groups data entries by user ID, which is extracted from the entry. This ensures that subsequent operations are performed within each user group.
Reduce Transformation:

**Operation: result.reduce((value1, value2) -> value1 + "; " + value2)**

**Purpose**: Concatenates all entries for each user ID, separated by "; ". This is useful for aggregating or combining related records.
```