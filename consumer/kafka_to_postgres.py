#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for Real-Time Stock Data
Reads from Kafka, transforms, and writes to PostgreSQL
"""

import os                                     # For environment variable access
import sys                                  # For system exit and logging
import logging
from datetime import datetime                 # For timestamp handling
from pyspark.sql import SparkSession           # For Spark session management
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, 
    monotonically_increasing_id, when, lit, coalesce
)                                                          # For DataFrame transformations
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, LongType, DecimalType, TimestampType
)                                                                               # For defining schema and data types
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException        # For streaming query management and exception handling
from pyspark.sql.utils import AnalysisException                                 # For handling analysis exceptions

# Configure logging
logging.basicConfig(                                                      
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',             # Log format with timestamp, logger name, log level, and message
    handlers=[
        logging.FileHandler('/opt/spark/logs/consumer.log'),                   
        logging.StreamHandler(sys.stdout)
    ]
)                                                                         # Log to both file and console
logger = logging.getLogger(__name__)                                      # Logger for this module                            

class StockDataStreamingConsumer:                                        
    """Spark Structured Streaming consumer for stock data"""
    
    def __init__(self):
        """Initialize Spark session and configuration"""
        self.app_name = "StockPriceStreaming"
        self.master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", 
            "kafka:9092"
        )
        self.kafka_topic = os.getenv("KAFKA_TOPIC", "stock_prices")
        self.kafka_offset = os.getenv("KAFKA_OFFSET", "latest")
        
        # PostgreSQL configuration
        self.postgres_host = os.getenv("POSTGRES_HOST", "postgres")
        self.postgres_port = os.getenv("POSTGRES_PORT", "5432")
        self.postgres_db = os.getenv("POSTGRES_DB", "stockdb")
        self.postgres_user = os.getenv("POSTGRES_USER", "stockuser")
        self.postgres_password = os.getenv("POSTGRES_PASSWORD", "stockpass")
        
        # Checkpoint locations
        self.dim_checkpoint = os.getenv(
            "DIM_CHECKPOINT_PATH", 
            "/opt/spark/checkpoints/dim_stock"
        )
        self.fact_checkpoint = os.getenv(
            "FACT_CHECKPOINT_PATH", 
            "/opt/spark/checkpoints/fact_stock"
        )
        
        # Initialize Spark
        self.spark = self._create_spark_session()
        
        # Define schema for JSON data
        self.stock_schema = self._define_schema()
        
    def _create_spark_session(self):
        """Create and configure Spark session"""
        logger.info("Initializing Spark session...")
        
        return SparkSession.builder \
            .appName(self.app_name) \
            .master(self.master_url) \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.sql.streaming.metricsEnabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kafka.consumer.cache.enabled", "false") \
            .config("spark.kafka.consumer.cache.timeout", "5m") \
            .getOrCreate()
    
    def _define_schema(self):
        """Define the schema for incoming JSON data"""
        return StructType([
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("change", DoubleType(), True),
            StructField("change_percent", DoubleType(), True),
            StructField("day_high", DoubleType(), True),
            StructField("day_low", DoubleType(), True),
            StructField("event_time", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("sector", StringType(), True)
        ])
    
    def _get_postgres_jdbc_url(self):
        """Construct PostgreSQL JDBC URL"""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    def _get_postgres_properties(self):
        """Get PostgreSQL connection properties"""
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
            "url": self._get_postgres_jdbc_url(),
            "stringtype": "unspecified",
            "reWriteBatchedInserts": "true",
            "batchsize": "1000"
        }
    
    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")
        
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", self.kafka_offset) \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .option("kafka.consumer.commit.offsets.enabled", "true") \
            .option("kafka.consumer.auto.offset.reset", "latest") \
            .option("kafka.consumer.enable.auto.commit", "true") \
            .option("kafka.consumer.isolation.level", "read_committed") \
            .load()
    
    def parse_and_transform(self, kafka_df):
        """Parse JSON and transform the data"""
        logger.info("Parsing and transforming streaming data...")
        
        # Parse JSON
        parsed_df = kafka_df \
            .select(
                from_json(
                    col("value").cast("string"), 
                    self.stock_schema
                ).alias("data")
            ) \
            .select("data.*")
        
        # Handle null values and malformed records
        transformed_df = parsed_df \
            .withColumn("event_time", 
                       to_timestamp(
                           coalesce(col("event_time"), 
                                   lit(datetime.now().isoformat())), 
                           "yyyy-MM-dd'T'HH:mm:ss"
                       )) \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("load_time", current_timestamp()) \
            .withColumn("price", 
                       col("price").cast(DecimalType(10,2))) \
            .withColumn("volume", 
                       when(col("volume").isNotNull(), 
                            col("volume").cast(LongType()))
                       .otherwise(lit(0))) \
            .withColumn("day_high", 
                       col("day_high").cast(DecimalType(10,2))) \
            .withColumn("day_low", 
                       col("day_low").cast(DecimalType(10,2))) \
            .withColumn("change", 
                       col("change").cast(DecimalType(10,2))) \
            .withColumn("change_percent", 
                       col("change_percent").cast(DecimalType(5,2))) \
            .withColumn("company_name", 
                       coalesce(col("company_name"), col("symbol"))) \
            .withColumn("sector", 
                       coalesce(col("sector"), lit("Unknown"))) \
            .filter(col("symbol").isNotNull()) \
            .filter(col("price").isNotNull())
        
        # Add unique ID using hash of symbol and timestamp
        final_df = transformed_df 
           # .withColumn("id", 
                      # monotonically_increasing_id())
        
        return final_df
    
    def prepare_dimension_data(self, df):
        """Prepare dimension table data"""
        return df \
            .select(
                col("symbol"),
                col("company_name"),
                col("sector")
            ) \
            .dropDuplicates(["symbol"]) \
            .withColumn("updated_at", current_timestamp())
    
    def prepare_fact_data(self, df):
        """Prepare fact table data"""
        return df.select(
            col("symbol"),
            col("price"),
            col("volume"),
            col("day_high"),
            col("day_low"),
            col("change"),
            col("change_percent"),
            col("event_time"),
            col("processing_time"),
            col("load_time")
        )
    
    def write_dimension_to_postgres(self, df, epoch_id):
        """Write dimension data to PostgreSQL with UPSERT logic"""
        logger.info(f"Writing dimension data for epoch: {epoch_id}")
        
        if df.count() == 0:
            logger.info("No dimension data to write")
            return
        
        props = self._get_postgres_properties()
        
        # Create temporary view
        df.createOrReplaceTempView("temp_dim_stock")
        
        # Use MERGE/UPSERT logic
        merge_query = """
            MERGE INTO dim_stock AS target
            USING temp_dim_stock AS source
            ON target.symbol = source.symbol
            WHEN MATCHED THEN
                UPDATE SET 
                    company_name = source.company_name,
                    sector = source.sector,
                    updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (symbol, company_name, sector, updated_at)
                VALUES (source.symbol, source.company_name, 
                        source.sector, source.updated_at)
        """
        
        try:
            # Write to PostgreSQL using JDBC
            df.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", props["url"]) \
                .option("dbtable", "dim_stock") \
                .option("user", props["user"]) \
                .option("password", props["password"]) \
                .option("driver", props["driver"]) \
                .option("stringtype", "unspecified") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} dimension records")
            
        except Exception as e:
            logger.error(f"Error writing dimension data: {str(e)}")
            raise
    
    def write_fact_to_postgres(self, df, epoch_id):
        """Write fact data to PostgreSQL"""
        logger.info(f"Writing fact data for epoch: {epoch_id}")
        
        if df.count() == 0:
            logger.info("No fact data to write")
            return
        
        props = self._get_postgres_properties()
        
        try:
            # Write to PostgreSQL using JDBC with batching
            df.write \
                .mode("append") \
                .format("jdbc") \
                .option("url", props["url"]) \
                .option("dbtable", "fact_stock_prices") \
                .option("user", props["user"]) \
                .option("password", props["password"]) \
                .option("driver", props["driver"]) \
                .option("batchsize", "1000") \
                .option("numPartitions", "2") \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully wrote {df.count()} fact records")
            
        except Exception as e:
            logger.error(f"Error writing fact data: {str(e)}")
            raise
    
    def run(self):
        """Execute the streaming pipeline"""
        logger.info("Starting stock data streaming consumer...")
        
        try:
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Parse and transform
            transformed_df = self.parse_and_transform(kafka_df)
            
            # Prepare dimension and fact data
            dim_df = self.prepare_dimension_data(transformed_df)
            fact_df = self.prepare_fact_data(transformed_df)
            
            # Write dimension stream
            dim_query = dim_df.writeStream \
                .foreachBatch(self.write_dimension_to_postgres) \
                .outputMode("update") \
                .trigger(processingTime="10 seconds") \
                .option("checkpointLocation", self.dim_checkpoint) \
                .queryName("dimension_writer") \
                .start()
            
            logger.info(f"Dimension stream started with checkpoint: {self.dim_checkpoint}")
            
            # Write fact stream
            fact_query = fact_df.writeStream \
                .foreachBatch(self.write_fact_to_postgres) \
                .outputMode("append") \
                .trigger(processingTime="5 seconds") \
                .option("checkpointLocation", self.fact_checkpoint) \
                .queryName("fact_writer") \
                .start()
            
            logger.info(f"Fact stream started with checkpoint: {self.fact_checkpoint}")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
            
        except StreamingQueryException as e:
            logger.error(f"Streaming query failed: {str(e)}")
            raise
        except AnalysisException as e:
            logger.error(f"Analysis error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        
        try:
            # Stop all streaming queries
            for query in self.spark.streams.active:
                query.stop()
            
            # Stop Spark session
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped successfully")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Main entry point"""
    consumer = StockDataStreamingConsumer()
    
    try:
        consumer.run()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        consumer.cleanup()
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()