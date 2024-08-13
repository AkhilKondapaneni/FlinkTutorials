
# CEPWithFileSource

This project demonstrates an Apache Flink job that uses Complex Event Processing (CEP) to detect high-value consecutive transactions from a CSV file. The job reads transaction data, applies a CEP pattern to identify users with consecutive high transactions, and prints alerts for such users.

## Overview

The `CEPWithFileSource` class illustrates how to:
- Set up a Flink execution environment for local execution.
- Read and parse transaction data from a CSV file.
- Apply a CEP pattern to detect users with consecutive high-value transactions within a specified time window.
- Print alerts for detected patterns.

## Features

- **CSV Parsing**: Reads and parses transaction data from a CSV file.
- **CEP Pattern**: Detects users with high consecutive transactions within a 10-minute window.
- **Local Execution**: Runs the job locally with a parallelism of 1.

## Requirements

- Apache Flink (compatible with the version used in this project)
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

## Configure Input Data

Place the `cep_transactions.csv` file in the `src/main/resources` directory. The CSV file should have the following format:

```
user_id,transaction_id,timestamp,amount
user1,1,2024-08-12 10:00:00,250.00
user1,2,2024-08-12 10:05:00,150.00
user2,3,2024-08-12 11:00:00,300.00
...
```

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.cep.CEPWithFileSource target/flink-tutorial-1.1-<version>.jar
```

## Code Overview

**CEPWithFileSource**: Main class to run the Flink job with CEP.

- **Data Parsing**: Reads the CSV file and parses each line into a `Tuple4` containing user ID, transaction ID, timestamp, and transaction amount.
- **Timestamp Assignment**: Assigns timestamps and watermarks to handle event time processing.
- **CEP Pattern Definition**:
    - **Pattern**: Detects users with high-value transactions (over 200.00) occurring consecutively within a 10-minute window.
- **Pattern Matching**:
    - **Select Function**: Formats the alert message for detected patterns.

## Data Transformations Explained

### CSV Parsing

**Operation: `transactions.map(line -> new Tuple4<>(userId, transactionId, timestamp, amount))`**

**Purpose:** Converts raw CSV lines into a structured format suitable for event processing.

### Timestamp Assignment

**Operation: `timestampedTransactions.assignTimestampsAndWatermarks(...)`**

**Purpose:** Assigns timestamps to events and generates watermarks to handle event time processing.

### CEP Pattern

**Operation: `Pattern.<Tuple4<String, Integer, LocalDateTime, Double>>begin("start").where(...)`**

**Purpose:** Defines a pattern to detect consecutive high-value transactions by the same user within 10 minutes.

### Pattern Matching

**Operation: `CEP.pattern(...).select(...)`**

**Purpose:** Applies the defined pattern to the data stream and generates alerts for detected patterns.

## Dependencies

- Apache Flink
- CSV File Processing

---
