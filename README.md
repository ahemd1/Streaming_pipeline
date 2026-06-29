# Realtime Data Streaming | End-to-End Data Engineering Project

An end-to-end real-time data engineering pipeline that orchestrates data ingestion, processing, and distributed storage. This architecture leverages Apache Airflow for workflow orchestration, Apache Kafka running in KRaft mode for high-throughput event streaming, Apache Spark for real-time stream processing, and Apache Cassandra for scalable NoSQL storage. The entire ecosystem is containerized using Docker for seamless deployment and scalability.

---

## System Architecture
![system Architecture](https://github.com/ahemd1/Streaming_pipeline/blob/main/System%20Architecture1.png)

The pipeline is engineered with a modern, decoupled architecture ensuring fault tolerance and high availability:

* **Data Ingestion (API)**: Fetches dynamic, random user profiles from the `randomuser.me` API.
* **Orchestration (Apache Airflow & PostgreSQL)**: Airflow DAGs automate and schedule the ingestion pipeline. Ingested data states and metadata are maintained within a PostgreSQL database.
* **Event Streaming (Apache Kafka - KRaft Mode)**: Functions as the central message broker. This setup eliminates the legacy ZooKeeper dependency, utilizing Kafka's native **KRaft** consensus mechanism where the broker and controller roles are unified within a single node.
* **Schema Governance & Monitoring**: Integrated with **Confluent Schema Registry** for strict schema management and **Confluent Control Center** for real-time stream monitoring.
* **Stream Processing (Apache Spark)**: A distributed Spark Structured Streaming application (Master-Worker architecture) consumes raw data from Kafka, applies structured transformations, and parses the JSON payloads against a defined schema.
* **Analytical Storage (Apache Cassandra)**: The transformed, structured records are streamed continuously and persisted into a distributed Cassandra NoSQL keyspace designed for high-write throughput.

---

## Tech Stack

* **Orchestration**: Apache Airflow 2.10.3
* **Metadata Database**: PostgreSQL 13 (Alpine)
* **Event Streaming**: Apache Kafka (Confluent Platform 7.4.0 - KRaft Mode)
* **Stream Processing**: Apache Spark 3.5.0 (Structured Streaming)
* **NoSQL Database**: Apache Cassandra (Latest)
* **Containerization**: Docker & Docker Compose
* **Language**: Python 3.11.6 (PySpark)

---

## Key Technical Features

* **ZooKeeperless Kafka**: Implemented modern cluster coordination via KRaft (`KAFKA_PROCESS_ROLES: 'broker,controller'`), reducing metadata synchronization overhead and infrastructure complexity.
* **Graceful Orchestration**: Utilized Airflow health checks (`service_healthy` conditions) to guarantee that core streaming services are fully operational before initiating ingestion DAGs.
* **Structured Real-Time Processing**: Leveraged PySpark's `readStream` and `writeStream` to parse incoming Kafka byte payloads into strongly-typed dataframes on the fly.

---

## Getting Started

### Prerequisites

Ensure you have the following installed on your host machine:
* Docker Desktop
* Git

### Installation & Deployment

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/ahemd1/Streaming_pipeline.git](https://github.com/ahemd1/Streaming_pipeline.git)
   cd Streaming_pipeline
