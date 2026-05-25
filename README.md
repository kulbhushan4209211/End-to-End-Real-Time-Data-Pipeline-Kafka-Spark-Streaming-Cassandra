
# End-to-End Real-Time Data Pipeline: Kafka, Spark Streaming, & Cassandra

A robust, fully containerized real-time streaming pipeline that ingests, cleans, and stores user data continuously. This project demonstrates modern Data Engineering practices, including handling continuous data streams, managing distributed system resources, and performing complex data sanitization on the fly.

## 🏗️ Architecture Overview

The pipeline is built using a microservices architecture orchestrated via Docker Compose:

1.  **Data Generation:** Apache Airflow simulates and pushes continuous user data payloads into a Kafka topic.
2.  **Message Queue:** Confluent Kafka acts as the distributed event streaming platform, decoupling data generation from processing.
3.  **Stream Processing:** Apache Spark (PySpark Structured Streaming) acts as the real-time processing engine. It reads the raw Kafka byte stream, extracts nested JSON using dynamic Regular Expressions, handles schema casting, and drops malformed records.
4.  **NoSQL Sink:** The sanitized micro-batches are written continuously into an Apache Cassandra cluster for high-availability querying.

## 🛠️ Tech Stack

* **Language:** Python 3, PySpark, CQL
* **Orchestration & Compute:** Apache Spark (Pinned v3.5.0 cluster with custom Ivy/memory configurations)
* **Message Broker:** Apache Kafka (Confluent)
* **Database:** Apache Cassandra (NoSQL)
* **Infrastructure:** Docker & Docker Compose (WSL2)

## 🚀 Key Features

* **Resilient Streaming:** Configured with `PERMISSIVE` modes and `latest` offsets to gracefully handle malformed Kafka messages (e.g., escaped JSON byte strings) without crashing the JVM.
* **Dynamic Data Sanitization:** Utilizes Spark SQL `regexp_replace` and `regexp_extract` to surgically extract valid JSON dictionaries from corrupted payload headers before parsing.
* **Automated Infrastructure:** The `spark_stream.py` script automatically establishes the Cassandra connection, initializes the `spark_streams` keyspace, and defines the schema, requiring zero manual database setup.
* **Custom Cluster Resource Management:** Explicitly handles worker/executor memory allocations to prevent resource deadlocks on local hardware.


## 🧠 Engineering Challenges Solved

* **Dependency Resolution (Ivy/Maven):** Resolved `/nonexistent` directory bugs within the Spark image by overriding Java options (`-Divy.home=/tmp`) globally for both drivers and executors.
* **Version Pinning & Compatibility:** Eliminated `NoSuchMethodError` crashes by enforcing strict version parity between Spark base images (3.5.0) and Scala dependencies (2.12).
* **Byte String Extraction:** Overcame Kafka serialization issues where payloads were wrapped in Python byte string notation (`b'...'`) and heavily escaped (`\"`), using dynamic PySpark RegEx to extract the clean JSON payload.
