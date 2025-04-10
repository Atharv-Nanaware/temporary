# Real-Time Data Engineering Pipeline

A production-ready data engineering solution that implements a complete ETL pipeline with real-time streaming capabilities. This project demonstrates advanced data engineering concepts including data extraction, transformation, validation, streaming processing, and storage.

## 🚀 Features

- **Real-time data streaming** with Apache Kafka
- **Distributed processing** with Apache Spark
- **Workflow orchestration** with Apache Airflow
- **NoSQL database storage** with Apache Cassandra
- **Containerized deployment** with Docker and Docker Compose
- **Data quality validation** with custom validation rules
- **Modular architecture** for maintainability and scalability

## 🏗️ Architecture

The pipeline consists of the following components:

1. **Data Extraction**: Fetches user data from the RandomUser API
2. **Data Transformation**: Processes and formats the raw data
3. **Data Validation**: Applies quality checks to ensure data integrity
4. **Kafka Producer**: Sends validated data to Kafka topics
5. **Spark Streaming**: Processes data streams in real-time
6. **Data Quality**: Additional quality checks before database storage
7. **Cassandra Storage**: Persists processed data in a NoSQL database

## 📂 Project Structure

```
project/
├── dags/                  # Airflow DAG definitions
│   └── realtime_streaming.py
├── pipelines/             # ETL pipeline components
│   ├── etl_pipeline.py    # Main ETL logic
│   └── data_validator.py  # Data validation rules
├── spark_streaming/       # Spark streaming components
│   ├── spark_processing.py    # Main Spark application
│   ├── spark_utils.py         # Spark and Kafka utilities
│   ├── cassandra_utils.py     # Cassandra database operations
│   └── data_quality.py        # Data quality checks
├── docker-compose.yaml    # Docker services configuration
└── dependencies.zip       # Python dependencies for Spark
```

## 🛠️ Technologies Used

- **Apache Airflow**: Workflow orchestration
- **Apache Kafka**: Message streaming platform
- **Apache Spark**: Distributed data processing
- **Apache Cassandra**: NoSQL database
- **Docker**: Containerization
- **Python**: Programming language
- **RandomUser API**: Data source

## 🚦 Getting Started

### Prerequisites

- Docker and Docker Compose
- Git

### Installation and Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Start the services:
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```

3. Copy dependencies to Spark:
   ```bash
   docker cp dependencies.zip spark-master:/dependencies.zip
   ```

4. Copy Spark streaming files:
   ```bash
   docker cp spark_streaming/ spark-master:/opt/spark/spark_streaming/
   ```

5. Create Kafka topic:
   ```bash
   docker exec -it broker kafka-topics.sh --create --topic user_profile_stream --bootstrap-server broker:9092 --partitions 1 --replication-factor 1
   ```

### Running the Pipeline

1. Start the Spark streaming application:
   ```bash
   docker exec -it spark-master spark-submit \
       --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
       --py-files /dependencies.zip,/opt/spark/spark_streaming/spark_utils.py,/opt/spark/spark_streaming/cassandra_utils.py,/opt/spark/spark_streaming/data_quality.py \
       /opt/spark/spark_streaming/spark_processing.py
   ```

2. Trigger the Airflow DAG:
   ```bash
   docker exec -it atharv_final-airflow-webserver-1 airflow dags trigger realtime_user_streaming_pipeline
   ```

3. View the results in Cassandra:
   ```bash
   docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streaming.created_users;"
   docker exec -it cassandra cqlsh -e "SELECT * FROM spark_streaming.created_users LIMIT 5;"
   ```

## 📊 Data Flow

1. Airflow DAG triggers the ETL process
2. Data is extracted from the RandomUser API
3. Raw data is transformed into a structured format
4. Data validation rules are applied
5. Valid records are sent to Kafka
6. Spark Streaming consumes data from Kafka
7. Additional data quality checks are performed
8. Clean data is written to Cassandra

## 🔍 Data Quality

The pipeline implements two levels of data quality checks:

1. **Airflow-level validation**:
   - Required fields validation
   - Email format validation
   - Date format validation
   - Data type validation
   - Value constraints validation

2. **Spark-level validation**:
   - Required fields validation
   - Email format validation
   - Date format validation
   - Gender value validation
   - Username length validation

## 🧩 Modularity

The project follows a modular design pattern:

- **ETL Pipeline**: Handles data extraction, transformation, and loading
- **Spark Utilities**: Manages Spark session and Kafka connections
- **Cassandra Utilities**: Handles database operations
- **Data Quality**: Implements validation rules and quality checks

## 🔧 Troubleshooting

Common issues and solutions:

- **No data in Cassandra**: Check Kafka topic creation and data validation rules
- **Spark job fails**: Verify dependencies and network connectivity
- **Airflow DAG fails**: Check logs for specific task failures

## 🚀 Future Enhancements

Potential improvements for the project:

- Implement monitoring and alerting
- Add unit and integration tests
- Implement CI/CD pipeline
- Add data lineage tracking
- Scale with Kubernetes orchestration

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 👤 Author

Atharv Nanaware - Data Engineer

---

*This project demonstrates advanced data engineering concepts and best practices for building real-time data pipelines.*
