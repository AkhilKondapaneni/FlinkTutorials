
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

We will also see how checkpoint are of use here during an unexpected task failures.

The `DeltaLakeSinkWithoutPartitionJob` class takes care of generating Customer records based on the configured count and publishes the data files into the configured storage location. Note, the data fiels here are not partitioned.

The `DeltaLakeSinkWithPartitionJob` class takes care of generating Customer records based on the configured count and publishes the data files into the configured storage location by partitioning against the location where the customer belongs. Note, the data fiels here are partitioned by Zip Code.

The `DeltaLakeSourceJob` class consumes the partitioned data files stored in delta lake format and prints as standard output. This job is enabled with Checkpoints and we will also see how the checkpoints are of use during a task failure.

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

### Lets sink some data to delta tables without partitioning
1. Run the following command to sink data to detla tables. The number of records and the table name is derieved from the delta-lake.properties file set above.
   The data being sinked is of the format:
        {"name": <value>, "age": <value>, "email": <value>, "zip": <value>, "isActive": <boolean value>}

2. Run the following command to sink data
```bash
docker exec -it flink-jobmanager ./bin/flink run --detached -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSinkWithoutPartitionJob /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
```

#### Verification
1. docker exec -it flink-taskmanager bash 
2. ls /opt/flink/delta-lake/<tablename configured in properties + 'without_partition'>

You will see the parquet files being stored here

### Lets sink some data to delta tables by partitioning based on zip code
1. Run the following command to sink data to detla tables. The number of records and the table name is derieved from the delta-lake.properties file set above.
   The data being sinked is of the format:
        {"name": <value>, "age": <value>, "email": <value>, "zip": <value>, "isActive": <boolean value>}

2. Run the following command to sink data
```bash
docker exec -it flink-jobmanager ./bin/flink run --detached -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSinkWithPartitionJob /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
```

#### Verification
1. docker exec -it flink-taskmanager bash 
2. ls /opt/flink/delta-lake/<tablename configured in properties + 'with_partition'>

You will see the parquet files being stored here. Is this different from what you saw earlier?

### Now lets read the data that has been sinked
1. Run the following command to read the data from the sinked tables.
```bash
docker exec -it flink-jobmanager ./bin/flink run --detached -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSourceJob /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
```
2. Navigate to the webUI of Flink @ http://localhost:9081/
3. Click on the 'Task Managers' link in the left panel and select the 'Logs' tab.
4. Scroll to the bottom to see the occurances of records those were read. Should be of this format:
     'Name: User74, Age: 36, Email: user5@example.com, Zip: 94105, isActive: false, ZipCodeUserCount: 7'
5. The last attribute 'ZipCodeUserCount' symbolizes the number of users belonging to the zip code against 'Zip' attribute.

### Now lets play around Fault Tolerance
1. Assuming the above job(to read data) is still running, lets stop it to see what happens.
```bash
docker compose kill flink-taskmanager
```
2. Navigate to the Flink WebUI @ http://localhost:9081/
3. Under the 'Running Jobs' menu, you will find the job named 'Flink Delta Lake Source Job' running. The job will try to restart as there are no task slots available to be assigned.
4. Under the 'Task Managers' menu, you will not find any task managers running.
5. Lets start the taskManager
```bash
docker compose up -d flink-taskmanager
```
6. The job 'Flink Delta Lake Source Job' should be healthy now with the tasks assigned to the new TaskManager spun.
7. Lets stream few more events now
```bash
docker exec -it flink-jobmanager ./bin/flink run --detached -c org.psyncopate.flink.connectors.deltalake.DeltaLakeSinkWithPartitionJob /opt/flink/jars/flink-tutorial-1.1-SNAPSHOT.jar
```
8. Navigate to the webUI of Flink @ http://localhost:9081/
9. Click on the 'Task Managers' link in the left panel and select the 'Logs' tab.
10. Scroll to the bottom to see the occurances of records those were read. Should be of this format:
     'Name: User74, Age: 36, Email: user5@example.com, Zip: 94105, isActive: false, ZipCodeUserCount: 7'
11. The last attribute 'ZipCodeUserCount' now appears to continue the counter from the last iteration though there was a failure occured in the task.



## Dependencies

- Apache Flink
- DeltaLake Source and Sink Connector

---