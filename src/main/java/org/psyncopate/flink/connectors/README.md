
# Third party connectors

This project demonstrates Apache Flink jobs that uses third party connectors to source and sink data through Flink.
- MongoDB Connector
- DeltaLake Connector

# MongoDB Source and Sink Connector
## Overview

The `MongoDBDataGeneratorJob` class takes care of generating Customer documents based on the configured count and publishes them into the MongoDB collection - Uses MongoDB Sink connector here.
The `MongoDBDataEnricherJob` class reads data from the collection sinked above and performs the following
- Filters only the active users
- Adds an attribute named 'processed' to every customer document that is active

## Requirements

- Apache Flink (compatible with the version used in this project)
- Java 11 or higher
- MongoDB Instance

## Setup

**Clone the Repository**

```bash
git clone <repository-url>
cd <repository-directory>
```

## Build

### Update mongodb.properties
Navigate to /src/main/resources/mongodb.properties
Update the following
- mongodb.uri=mongodb://<username>:<pwd>@mongodb-container:27017/<database_name>
- mongodb.database=<database_name>
- mongodb.collection=<collection_name>
- no.of.documents=<number_of_documents_to_be_inserted>

```bash
mvn clean package
```

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.connectors.mongodb.MongoDBDataGeneratorJob target/flink-tutorial-1.1-<version>.jar
flink run -c org.psyncopate.flink.connectors.mongodb.MongoDBDataEnricherJob target/flink-tutorial-1.1-<version>.jar
```

## Dependencies

- Apache Flink
- MongoDB Source and Sink Connector

---

# Delta Lake Source and Sink Connector
## Overview

Flink/Delta Connector is a JVM library to read and write data from Apache Flink applications to Delta tables utilizing the Delta Standalone JVM library. The connector provides exactly-once delivery guarantees.
The `DeltaLakeSinkWithoutPartitionJob` class takes care of generating Customer records based on the configured count and publishes the data files into the configured storage location. Note, the data fiels here are not partitioned.

The `DeltaLakeSinkWithPartitionJob` class takes care of generating Customer records based on the configured count and publishes the data files into the configured storage location by partitioning against the location where the customer belongs. Note, the data fiels here are partitioned by Zip Code.

The `DeltaLakeSourceJob` class consumes the partitioned data files stored in delta lake format and prints as standard output.

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

### Update delta-lake.properties
Navigate to /src/main/resources/delata-lake.properties
Update the following
- deltaTable.path==<Obsolute path of local filesystem where the tables are to be stored>
- deltaTable.noOfRecords=<number of records to be pushed to data lake>

```bash
mvn clean package
```

## Running the Job

Execute the Flink job using the following command:

```bash
flink run -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSinkWithoutPartitionJob target/flink-tutorial-1.1-<version>.jar
flink run -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSinkWithoutPartitionJob target/flink-tutorial-1.1-<version>.jar
flink run -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSourceJob target/flink-tutorial-1.1-<version>.jar
```

## Dependencies

- Apache Flink
- DeltaLake Source and Sink Connector

---