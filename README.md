
# Kafka and Flink Infrastructure Setup

This document outlines the steps to set up a Kafka and Flink infrastructure using Docker Compose. The setup includes Kafka, Schema Registry, Kafka Connect, Control Center, ksqlDB, Flink, and other related services.

## Prerequisites

- **Docker**: Ensure Docker is installed and running on your machine.
- **Docker Compose**: Ensure Docker Compose is installed.

## Setup Instructions

### 1. Clone the Repository

First, clone the repository where the Docker Compose file is located:

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Prepare Docker Compose Configuration

Ensure that the `docker-compose.yml` file is placed in the `src/main/resources` directory. This file contains configurations for all the necessary services:

- **Kafka Broker**
- **Schema Registry**
- **Kafka Connect**
- **Control Center**
- **ksqlDB Server and CLI**
- **Kafka REST Proxy**
- **Flink SQL Client, Job Manager, and Task Manager**

### 3. Start the Docker Containers

Navigate to the directory containing `docker-compose.yml` and start the services:

```bash
cd /src/main/resources
docker compose up -d
```

This command will start all the defined services, including Kafka, Schema Registry, Control Center, ksqlDB, and Flink.

### 4. Verify the Setup

Once the containers are up and running, verify the services by accessing the following endpoints:

- **Kafka Broker**: [http://localhost:9092](http://localhost:9092)
- **Schema Registry**: [http://localhost:8081](http://localhost:8081)
- **Kafka Connect**: [http://localhost:8083](http://localhost:8083)
- **Control Center**: [http://localhost:9021](http://localhost:9021)
- **ksqlDB Server**: [http://localhost:8088](http://localhost:8088)
- **Kafka REST Proxy**: [http://localhost:8082](http://localhost:8082)
- **Flink SQL Client**: [http://localhost:8081](http://localhost:8081) (via Docker exec -it shell)
- **Flink JobManager**: [http://localhost:9081](http://localhost:9081)

### 5. Run Flink Jobs
To submit Flink jobs, use the Flink job manager url: [http://localhost:9081](http://localhost:9081)

To run FlinkSQL jobs, use the `flink-sql-client` container. You can interact with it via:

```bash
docker exec -it flink-sql-client /bin/sh
```

From within the container, you can execute SQL queries or submit jobs.

### 6. Stopping the Services

To stop the Docker containers, run:

```bash
docker compose down
```

This command will stop and remove all containers defined in the Docker Compose file.

## Troubleshooting

- **Containers Not Starting**: Check logs for individual services using `docker logs <container-name>`.
- **Network Issues**: Ensure Docker network settings are correctly configured.
- **Port Conflicts**: Ensure the ports used in `docker-compose.yml` are not conflicting with other services on your machine.

## License

This setup is provided for educational and development purposes. Refer to the Docker, Confluent Kafka, and Flink documentation for licensing and usage terms.
