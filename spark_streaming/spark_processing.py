import logging
from datetime import datetime
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streaming
            WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        logger.info("Keyspace created successfully")
    except Exception as e:
        logger.error(f"Error creating keyspace: {e}")


def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streaming.created_users (
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT PRIMARY KEY,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
        """)
        logger.info("Table created successfully")
    except Exception as e:
        logger.error(f"Error creating table: {e}")


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.cassandra.connection.host", "cassandra_db") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark connection: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        kafka_stream = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("subscribe", "user_profile_stream") \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info("Kafka dataframe created successfully")
        return kafka_stream
    except Exception as e:
        logger.error(f"Error creating Kafka dataframe: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(["cassandra_db"])
        session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return session
    except Exception as e:
        logger.error(f"Error creating Cassandra connection: {e}")
        return None


def define_schema():
    return StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True),
    ])


def create_selection_df_from_kafka(kafka_df):
    schema = define_schema()
    try:
        selection_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        logger.info("Selection dataframe created successfully")
        return selection_df
    except Exception as e:
        logger.error(f"Error creating selection dataframe: {e}")
        return None


if __name__ == "__main__":
    #  Establish Spark Connection
    spark_conn = create_spark_connection()
    if spark_conn is None:
        logger.error("Failed to establish Spark connection. Exiting...")
        exit(1)

    # Connect to Kafka and fetch the stream
    kafka_df = connect_to_kafka(spark_conn)
    if kafka_df is None:
        logger.error("Failed to connect to Kafka. Exiting...")
        exit(1)

    #  Parse and transform Kafka data
    selection_df = create_selection_df_from_kafka(kafka_df)
    if selection_df is None:
        logger.error("Failed to process Kafka data. Exiting...")
        exit(1)

    # Connection  to Cassandra
    cassandra_session = create_cassandra_connection()
    if cassandra_session is None:
        logger.error("Failed to connect to Cassandra. Exiting...")
        exit(1)

    #  Keyspace and Table
    create_keyspace(cassandra_session)
    create_table(cassandra_session)

    # Write stream to Cassandra
    try:
        query = selection_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .option("keyspace", "spark_streaming") \
            .option("table", "created_users") \
            .start()
        logger.info("Streaming query started successfully")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error running streaming query: {e}")


    cassandra_session.shutdown()