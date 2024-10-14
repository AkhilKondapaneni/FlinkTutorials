
# Third party connectors

This project demonstrates Apache Flink jobs that uses third party connectors to source and sink data through Flink.

# MongoDB Source and Sink Connector
## Overview

The `MongoDBDataGeneratorJob` class takes care of generating Customer documents based on the configured count and publishes them into the MongoDB collection - Uses MongoDB Sink connector here.
The `MongoDBDataEnricherJob` class reads data from the collection sinked above and performs the following
- Filters only the active users
- Adds an attribute named 'processed' to every customer document that is active

## Overview

The `CEPWithFileSource` class illustrates how to:
- Set up a Flink execution environment for local execution.
- Read and parse transaction data from a CSV file.
- Apply a CEP pattern to detect users with consecutive high-value transactions within a specified time window.
- Print alerts for detected patterns.

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
Go to /src/main/resources/mongodb.properties
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
- MongoDB Source Connector

---
