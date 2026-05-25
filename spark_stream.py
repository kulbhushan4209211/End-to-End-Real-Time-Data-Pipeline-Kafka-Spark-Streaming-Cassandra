import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_extract, regexp_replace
from pyspark.sql.avro.functions import from_avro  # Ensure correct import
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Configure standard console logging so you can see exceptions
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class' : 'SimpleStrategy','replication_factor': '1'};
    """)
    logging.info('Keyspace created successfully.')

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT,
            age TEXT,
            gender TEXT,
            username TEXT,
            picture TEXT
        )
    """)
    logging.info('Table created successfully.')

def create_spark_connection():
    spark_c = None
    try:
        # 🟢 CORRECTION: Aligned packages matching your Spark environment (v3.5.x ecosystem)
        # Added mandatory 'spark-avro' coordinate
        jar_paths = (
            "/opt/spark/jars/custom/spark-sql-kafka-0-10_2.12-3.5.6.jar,"
            "/opt/spark/jars/custom/spark-avro_2.12-3.5.6.jar,"
            "/opt/spark/jars/custom/spark-cassandra-connector_2.12-3.5.0.jar"
        )
        packages = (
            "org.apache.spark:spark-sql-kafka-0-10_2.12-3.5.6,"
            "org.apache.spark:spark-avro_2.12-3.5.6,"
            "com.datastax.spark:spark-cassandra-connector_2.12-3.5.0"
        )
        
        spark_c = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .config('spark.master', 'spark://spark-master:7077') \
            .getOrCreate()
            
        spark_c.sparkContext.setLogLevel("WARN")
        logging.info('Spark connection created successfully using local volume JARs!')
    except Exception as e:
        logging.error(f"Could not create spark session due to {e}")
    return spark_c

# def create_kafka_connection(spark_conn):
#     spark_df = None
#     try:
#         # Define the exact Avro Schema structure matching your Confluent Schema Registry registration
#         # Since from_avro requires the schema string, we provide it directly:
#         json_schema_str = """
#         {
#             "type": "record",
#             "name": "User",
#             "fields": [
#                 {"name": "id", "type": "string"},
#                 {"name": "name", "type": "string"},
#                 {"name": "email", "type": "string"},
#                 {"name": "phone", "type": "string"},
#                 {"name": "address", "type": "string"},
#                 {"name": "age", "type": "string"},
#                 {"name": "gender", "type": "string"},
#                 {"name": "username", "type": "string"},
#                 {"name": "picture", "type": "string"}
#             ]
#         }
#         """

#         # 🟢 CORRECTION: Strip Kafka's 5-byte Confluent wire-protocol magic header before parsing Avro
#         binary_df = spark_conn.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'broker1:29092') \
#             .option('subscribe', 'users') \
#             .option('startingOffsets', 'earliest') \
#             .load()

#         # Confluent Avro messages insert a 5-byte identifier header before payload bytes. 
#         # We slice it using expr("substring(value, 6, length(value)-5)")
#         raw_avro_df = binary_df.selectExpr("substring(value, 6, length(value)-5) as avro_value")

#         # Parse Avro payload and select nested fields out to top-level database columns
#         spark_df = raw_avro_df.select(from_avro(col("avro_value"), json_schema_str).alias("nested_data")) \
#             .select(
#                 col("nested_data.id").cast("string").alias("id"), # Will automatically parse into Cassandra UUID
#                 col("nested_data.name").alias("name"),
#                 col("nested_data.email").alias("email"),
#                 col("nested_data.phone").alias("phone"),
#                 col("nested_data.address").alias("address"),
#                 col("nested_data.age").alias("age"),
#                 col("nested_data.gender").alias("gender"),
#                 col("nested_data.username").alias("username"),
#                 col("nested_data.picture").alias("picture")
#             )
            
#         logging.info('Dataframe schema successfully extracted and mapped.')
#     except Exception as e:
#         logging.error(f"Dataframe compilation failed due to: {e}")
#     return spark_df

def create_kafka_connection(spark_conn):
    spark_df = None
    try:
        # Age is an integer in the raw JSON, so we must use IntegerType() here
        json_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("address", StringType(), True),
            StructField("age", IntegerType(), True), 
            StructField("gender", StringType(), True),
            StructField("username", StringType(), True),
            StructField("picture", StringType(), True)
        ])

        binary_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker1:29092') \
            .option('subscribe', 'users') \
            .option('startingOffsets', 'latest') \
            .load()

        # 1. Extract the dictionary with RegEx
        extracted_df = binary_df.withColumn("extracted_json", regexp_extract(col("value").cast("string"), "\\{.*\\}", 0))

        # 2. 🟢 THE FINAL FIX: Remove the backslashes (replace \" with ")
        clean_df = extracted_df.withColumn("clean_json", regexp_replace(col("extracted_json"), "\\\\\"", "\""))

        # 3. Parse the perfectly clean JSON and flatten the columns
        spark_df = clean_df.select(from_json(col("clean_json"), json_schema).alias("data")) \
            .select(
                col("data.id").alias("id"),
                col("data.name").alias("name"),
                col("data.email").alias("email"),
                col("data.phone").alias("phone"),
                col("data.address").alias("address"),
                col("data.age").cast("string").alias("age"), # Cast back to string because Cassandra table expects TEXT
                col("data.gender").alias("gender"),
                col("data.username").alias("username"),
                col("data.picture").alias("picture")
            )
            
        logging.info('Dataframe schema successfully extracted, unescaped, and parsed.')
    except Exception as e:
        logging.error(f"Dataframe compilation failed due to: {e}")
    return spark_df
def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Couldn't connect to Cassandra due to {e}")
        return None

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        df = create_kafka_connection(spark_conn)
        session = create_cassandra_connection()
    
        if session is not None and df is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Starting active Streaming Query routing to Cassandra...")
            
            # 🟢 CORRECTION: Attached .awaitTermination() so your python process remains active on the workers!
            # streaming_query = (df.writeStream 
            #                    .format("org.apache.spark.sql.cassandra")\
            #                    .option('checkpointLocation', 'tmp/checkpoint')\
            #                    .option('keyspace', 'spark_streams')\
            #                    .option('table', 'created_users')\
            #                 #    .trigger(availableNow=True)\
            #                    .start())

            logging.info("Starting active Streaming Query routing to Cassandra...")
            
            streaming_query = (df.writeStream 
                               .format("org.apache.spark.sql.cassandra")\
                               .option('checkpointLocation', '/tmp/checkpoint')\
                               .option('keyspace', 'spark_streams')\
                               .option('table', 'created_users')\
                               .start())
                               
            streaming_query.awaitTermination()
                               
            streaming_query.awaitTermination()