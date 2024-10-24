# Fault Tolerance Job

This project demonstrates a simple Apache Flink job that implements fault tolerance using checkpointing and state management. The job generates a stream of numbers, processes them to calculate a running sum, and writes the results to a text file.

## Project Structure

- **Languages**: Java
- **Build Tool**: Maven
- **Frameworks**: Apache Flink

## Prerequisites

- Java 11
- Apache Maven
- Docker (for running the required services)

## Setup

1. **Clone the repository**:
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Build and package the project**:
    ```sh
    mvn package
    ```

3. **Start the required services** using Docker Compose:
    ```sh
    docker compose up -d
    ```


## Code Overview

### Main Class: `FaultTolerance`

The main class is located at `src/main/java/org/psyncopate/flink/state/management/FaultTolerance.java`.

#### Key Components:

- **Execution Environment**: Sets up the Flink execution environment.
- **Checkpointing**: Enables checkpointing for fault tolerance with `CheckpointingMode.EXACTLY_ONCE`.
- **State Backend**: Optionally sets the state backend to RocksDB.
- **Data Stream**: Generates a stream of numbers and processes them and prints them.
- **Output**: Writes the processed results to a text file.

### Docker Compose

The `docker-compose.yml` file sets up the necessary services, including Kafka, Schema Registry, and Flink components.

## Configuration

- **Checkpoint Directory**: `file:///opt/flink/checkpoints`
- **Savepoint Directory**: `file:///opt/flink/savepoints`
- **Output File**: `output/sum_results.txt`

## Detailed Job Explanation

The `FaultTolerance` class is an Apache Flink job that demonstrates fault tolerance using checkpointing and state management. Here's a detailed explanation of the job:

### Overview
The job generates a stream of numbers, processes them to calculate a running sum, and writes the results to a text file. It includes fault tolerance mechanisms such as checkpointing and state backend configuration.

### Key Components

1. **Execution Environment**:
   - The execution environment is created using `StreamExecutionEnvironment.getExecutionEnvironment()`.
   - Parallelism is set to 1 to ensure the output is written to a single file.

2. **Checkpointing**:
   - Checkpointing is enabled with a checkpoint interval of 1000 milliseconds and `CheckpointingMode.EXACTLY_ONCE` to ensure fault tolerance.
   - The state backend is set to RocksDB, which stores state information on disk at the specified checkpoint directory (`file:///opt/flink/checkpoints`).

3. **Data Stream**:
   - A data stream of integers is created using `env.fromSequence(0, 20,000)`, which generates numbers from 0 to 20,000.
   - The stream is then mapped to emit 0s for the first 20,000 elements and a final 1000.

4. **Processing**:
   - The stream is processed and printed to console. 

5. **Output**:
   - The processed results are written to a text file (`output/sum_results.txt`) in overwrite mode.

6. **Execution**:
   - The job is executed with `env.execute("Fault Tolerance Demo")`.

## Demonstration

### Fault Tolerance with Checkpointing

1. **Submit the job to the cluster**:
    ```sh
    docker exec -it jobmanager /bin/bash
    flink run --detached /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
    ```

2. **Simulate a fault by killing the Task Manager container**:
    ```sh
    docker compose stop taskmanager
    ```

3. **Restart the Task Manager container**:
    ```sh
    docker compose start taskmanager
    ```

The job will recover from the last checkpoint and continue processing.

### Savepointing

1. **Submit the job**:
    ```sh
    docker exec -it jobmanager /bin/bash
    flink run --detached /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
    ```

2. **Stop the job gracefully with a savepoint**:
    ```sh
    flink stop --savepointPath /opt/flink/savepoints <job-id>
    ```

3. **Resubmit the job with the savepoint location**:
    ```sh
    flink run -s /opt/flink/savepoints/savepoint-<savepoint-id> /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
    ```

The job will resume from the savepoint and continue processing.

## License

This project is licensed under a custom license. Usage is permitted only with explicit permission from Psyncopate Inc.

## Additional Resources

For more information, refer to the official [Apache Flink documentation](https://flink.apache.org/).