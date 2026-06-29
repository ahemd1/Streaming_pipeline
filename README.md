# Realtime Data Streaming | End-to-End Data Engineering Project

[cite_start]An end-to-end real-time data engineering pipeline that orchestrates data ingestion, processing, and distributed storage[cite: 20]. [cite_start]This architecture leverages Apache Airflow for workflow orchestration, Apache Kafka running in KRaft mode for high-throughput event streaming, Apache Spark for real-time stream processing, and Apache Cassandra for scalable NoSQL storage[cite: 20]. [cite_start]The entire ecosystem is containerized using Docker for seamless deployment and scalability[cite: 20].

---

## Project Structure

```text
├── dags/                  # Airflow DAGs for data ingestion
├── spark/                 # Spark Structured Streaming scripts
├── docker-compose.yml     # Multi-container Docker application configuration
└── README.md


System Architecture

[cite_start]The pipeline is engineered with a modern, decoupled architecture ensuring fault tolerance and high availability[cite: 20]:

[cite_start]Data Ingestion (API): Fetches dynamic, random user profiles from the randomuser.me API[cite: 20].

[cite_start]Orchestration (Apache Airflow & PostgreSQL): Airflow DAGs automate and schedule the ingestion pipeline[cite: 20]. [cite_start]Ingested data states and metadata are maintained within a PostgreSQL database[cite: 20].

[cite_start]Event Streaming (Apache Kafka - KRaft Mode): Functions as the central message broker[cite: 20]. [cite_start]This setup eliminates the legacy ZooKeeper dependency, utilizing Kafka's native KRaft consensus mechanism where the broker and controller roles are unified within a single node[cite: 20].

[cite_start]Schema Governance & Monitoring: Integrated with Confluent Schema Registry for strict schema management and Confluent Control Center for real-time stream monitoring[cite: 20].

[cite_start]Stream Processing (Apache Spark): A distributed Spark Structured Streaming application (Master-Worker architecture) consumes raw data from Kafka, applies structured transformations, and parses the JSON payloads against a defined schema[cite: 20].

[cite_start]Analytical Storage (Apache Cassandra): The transformed, structured records are streamed continuously and persisted into a distributed Cassandra NoSQL keyspace designed for high-write throughput[cite: 20].

Tech Stack

[cite_start]Orchestration: Apache Airflow 2.10.3 [cite: 20]

[cite_start]Metadata Database: PostgreSQL 13 (Alpine) [cite: 20]

[cite_start]Event Streaming: Apache Kafka (Confluent Platform 7.4.0 - KRaft Mode) [cite: 20]

[cite_start]Stream Processing: Apache Spark 3.5.0 (Structured Streaming) [cite: 20]

[cite_start]NoSQL Database: Apache Cassandra (Latest) [cite: 20]

[cite_start]Containerization: Docker & Docker Compose [cite: 20]

[cite_start]Language: Python 3.11.6 (PySpark) [cite: 20]

Key Technical Features

[cite_start]Native KRaft Consensus: Implemented modern Kafka cluster coordination utilizing KRaft (KAFKA_PROCESS_ROLES: 'broker,controller')[cite: 20]. [cite_start]This unifies broker and controller roles, drastically minimizing metadata synchronization overhead and infrastructure footprint[cite: 20].

[cite_start]Graceful Orchestration: Utilized Airflow health checks (service_healthy conditions) to guarantee that core streaming services are fully operational before initiating ingestion DAGs[cite: 20].

[cite_start]Structured Real-Time Processing: Leveraged PySpark's readStream and writeStream to parse incoming Kafka byte payloads into strongly-typed dataframes on the fly[cite: 20].

Getting Started

Prerequisites

[cite_start]Ensure you have the following installed on your host machine[cite: 20]:

[cite_start]Docker Desktop [cite: 20]

[cite_start]Git [cite: 20]

Installation & Deployment

Clone the repository:

git clone [https://github.com/ahemd1/Streaming_pipeline.git](https://github.com/ahemd1/Streaming_pipeline.git)
cd Streaming_pipeline


Start the Infrastructure:
Run Docker Compose in detached mode to spin up all services:

docker compose up -d
