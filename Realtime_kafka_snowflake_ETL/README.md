# Real-Time Data Pipeline Project

This project is designed to ingest, process, and analyze real-time data from multiple sources (API and MySQL database) using a robust streaming architecture. The goal is to collect, transform, and store data in a structured format for further analysis and visualization.

---

## 1. Overview of the Architecture

The project utilizes a combination of **Kafka**, **Kafka Connect**, **Snowflake**, and **Power BI** to create a scalable and efficient data pipeline.

### Data Sources

* **ClickStream API**: Hosted on EC2, providing clickstream data in JSON format.
* **MySQL Database**: Stores transaction logs.

###  Data Processing

* **Apache Kafka**: Acts as a central message broker to handle real-time data streams.
* **Kafka Connect**: Facilitates integration with external systems (API and MySQL).

### Data Storage & Transformation

* **Amazon S3**: Intermediate landing for data from Kafka.
* **Snowflake**: Central data warehouse to store raw, structured, and transformed data.
* **Snowpipe**: Automatically loads data into Snowflake from S3.

### Data Visualization

* **Power BI**: Connects to Snowflake to visualize transformed data.

---

## 2. Detailed Flow of the Project

### Step 1: Data Ingestion from API

* **Source**: API hosted on EC2.
* **Connector**: `HTTP Source Connector` in Kafka Connect.
* **Kafka Topic**: `clickstream-http-data`

### Step 2: Data Ingestion from MySQL

* **Source**: MySQL database containing transaction logs.
* **Connector**: `Debezium MySQL Connector` for CDC (Change Data Capture).
* **Kafka Topic**: `temp_sql.sales.orders`

###  Step 3: Data Storage in S3

* **Sink Connector**: `S3 Sink Connector` writes Kafka data to S3.
* **Clickstream Data Path**:
  `s3://snowflake-data-practice-bucket/topics/clickstream-http-data/`
* **Transaction Logs Path**:
  `s3://snowflake-data-practice-bucket/topics/temp_sql.sales.orders/`

### Step 4: Data Loading into Snowflake

* **External Stages**:

  * `api_ext_stage` → Clickstream data in S3
  * `logs_ext_stage` → Transaction logs in S3

* **Snowpipe** auto-loads data into:

  * `raw_events` (clickstream)
  * `raw_logs` (transaction logs)

* **Structured Tables**:

  * `structured_events`
  * `structured_logs`

* **Transformed Tables**:

  * `transformed_clickstream`
  * `transformed_logs`

### 🔄 Step 5: Data Transformation in Snowflake

* **Tasks Used**:

  * `insert_event_data_task`: Parses raw JSON from `raw_events` → `structured_events`
  * `task_load_transformed_clickstream`: Transforms `structured_events` → `transformed_clickstream`
    
*  Same process is used for loading MySQL data

### 📈 Step 6: Data Visualization

* **Power BI**:

  * Connects directly to Snowflake
  * Uses `transformed_clickstream` and `transformed_logs` for dashboards & reports

---

## 3. Key Components and Their Roles

###  a. Docker Compose Setup

The `docker-compose.yml` file includes the following services:

* **Kafka**:

  * Single-node Kafka cluster
  * Uses volume `kafka_data` for persistence

* **Schema Registry**:

  * Manages Avro schemas for data serialization

* **Kafka Connect**:

  * Includes connectors for:

    * Debezium MySQL
    * HTTP Source
    * S3 Sink
    * Elasticsearch, Neo4j, PostgreSQL (optional)

* **Kafka UI**:

  * Web UI to monitor Kafka clusters and topics

### 🔌 b. Kafka Connectors

* **MySQL Source Connector** (`mysql_src_connector.json`):

  * Captures CDC from `sales.orders`
  * Writes to topic `temp_sql.sales.orders`

* **HTTP Source Connector** (`CLICKSTREAM_SRC_CONNECTOR`):

  * Polls EC2-hosted API
  * Writes to topic `clickstream-http-data`

* **S3 Sink Connectors** (`sink_s3_connector`, `sink_mysql_connector`):

  * Writes Kafka topics to S3 buckets for:

    * Clickstream data
    * Transaction logs

###  c. Snowflake

* **Stages**:

  * `api_ext_stage` → Clickstream data
  * `logs_ext_stage` → Transaction logs

* **Tables**:

  * **Raw Tables**: `raw_events`, `raw_logs`
  * **Structured Tables**: `structured_events`, `structured_logs`
  * **Transformed Tables**: `transformed_clickstream`, `transformed_logs`

* **Pipes & Tasks**:

  * **Snowpipe**: Loads from S3 to raw tables
  * **Tasks**:

    * JSON parsing and transformation logic

### 📊 d. Power BI

* Directly connected to Snowflake
* Uses `transformed_` tables for reports and dashboards

---

## Tech Stack

| Component      | Technology     |
| -------------- | -------------- |
| Stream Broker  | Apache Kafka   |
| Connectors     | Kafka Connect  |
| Source DB      | MySQL          |
| External API   | JSON over HTTP |
| Storage        | Amazon S3      |
| Data Warehouse | Snowflake      |
| Visualization  | Power BI       |
| Orchestration  | Docker Compose |

---

## Project Outcomes

* Near real-time data processing
* Scalable pipeline with decoupled components
* CDC ingestion from databases
* Streamlined Snowflake loading and transformation
* Business-ready visualizations in Power BI

