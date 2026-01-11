#!/usr/bin/env python3
"""
PySpark Streaming Processor for M-Pesa Transactions

Consumes from Kafka, applies fraud detection rules, and aggregates data.
Fraud rules: amount > 10000, >5 transactions per user in window, location outliers.
"""

import json
import math
import time
from typing import Dict, Any, List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, avg, max, min,
    hour, minute, second, when, lit, udf, broadcast, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    BooleanType, LongType, IntegerType
)
from pyspark.streaming import StreamingContext
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, RequestError


class MPesaStreamProcessor:
    """PySpark streaming processor for M-Pesa transaction fraud detection"""
    
    def __init__(self, 
                 kafka_bootstrap_servers: str = 'localhost:9092',
                 kafka_topic: str = 'mpesa-transactions',
                 window_duration: str = '60 seconds',
                 slide_duration: str = '30 seconds',
                 checkpoint_location: str = 'data/checkpoints',
                 elasticsearch_host: str = 'localhost',
                 elasticsearch_port: int = 9200,
                 elasticsearch_index: str = 'mpesa-insights'):
        
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.window_duration = window_duration
        self.slide_duration = slide_duration
        self.checkpoint_location = checkpoint_location
        self.elasticsearch_host = elasticsearch_host
        self.elasticsearch_port = elasticsearch_port
        self.elasticsearch_index = elasticsearch_index
        
        # Nairobi coordinates for distance calculation
        self.nairobi_lat = -1.286389
        self.nairobi_long = 36.817223
        self.distance_threshold_km = 50.0
        
        # Initialize Spark
        self.spark = None
        self.setup_spark()
        
        # Initialize Elasticsearch
        self.es = None
        self.setup_elasticsearch()
        
        # Define schema for incoming transactions
        self.transaction_schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location", StructType([
                StructField("lat", DoubleType(), True),
                StructField("long", DoubleType(), True)
            ]), True),
            StructField("type", StringType(), True)
        ])
    
    def setup_spark(self):
        """Initialize SparkSession with proper configuration"""
        self.spark = SparkSession.builder \
            .appName("MPesaStreamProcessor") \
            .master("local[*]") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_location) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.receiver.maxRate", "1000") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("Spark session initialized successfully")
    
    def setup_elasticsearch(self):
        """Initialize Elasticsearch client with retry logic"""
        try:
            self.es = Elasticsearch([{
                'host': self.elasticsearch_host,
                'port': self.elasticsearch_port,
                'scheme': 'http'
            }])
            
            # Test connection
            if self.es.ping():
                print(f"Connected to Elasticsearch at {self.elasticsearch_host}:{self.elasticsearch_port}")
                self.create_index_with_mappings()
            else:
                raise ConnectionError("Failed to connect to Elasticsearch")
                
        except Exception as e:
            print(f"Failed to initialize Elasticsearch: {e}")
            raise
    
    def create_index_with_mappings(self):
        """Create Elasticsearch index with proper mappings for geo_point"""
        index_mapping = {
            "mappings": {
                "properties": {
                    "user_id": {"type": "keyword"},
                    "amount": {"type": "double"},
                    "timestamp": {"type": "date"},
                    "transaction_timestamp": {"type": "date"},
                    "location": {
                        "type": "geo_point"
                    },
                    "latitude": {"type": "double"},
                    "longitude": {"type": "double"},
                    "type": {"type": "keyword"},
                    "is_flagged": {"type": "boolean"},
                    "is_high_amount": {"type": "boolean"},
                    "is_location_outlier": {"type": "boolean"},
                    "distance_from_nairobi": {"type": "double"},
                    "hour": {"type": "integer"},
                    "total_transactions": {"type": "long"},
                    "total_volume": {"type": "double"},
                    "flagged_count": {"type": "long"},
                    "avg_amount": {"type": "double"},
                    "max_amount": {"type": "double"},
                    "min_amount": {"type": "double"},
                    "document_type": {"type": "keyword"}
                }
            }
        }
        
        try:
            # Delete index if it exists (for testing)
            if self.es.indices.exists(index=self.elasticsearch_index):
                self.es.indices.delete(index=self.elasticsearch_index)
                print(f"Deleted existing index: {self.elasticsearch_index}")
            
            # Create index with mappings
            self.es.indices.create(index=self.elasticsearch_index, body=index_mapping)
            print(f"Created Elasticsearch index: {self.elasticsearch_index}")
            
        except Exception as e:
            print(f"Failed to create index: {e}")
            raise
    
    def calculate_distance_from_nairobi(self, lat: float, long: float) -> float:
        """Calculate distance from Nairobi center using Haversine formula"""
        # Convert to radians
        lat1, lon1 = math.radians(self.nairobi_lat), math.radians(self.nairobi_long)
        lat2, lon2 = math.radians(lat), math.radians(long)
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Earth's radius in kilometers
        r = 6371.0
        return c * r
    
    def register_distance_udf(self):
        """Register UDF for distance calculation"""
        def distance_udf(lat: float, long: float) -> float:
            if lat is None or long is None:
                return 0.0
            return self.calculate_distance_from_nairobi(lat, long)
        
        return udf(distance_udf, DoubleType())
    
    def parse_transactions(self, df):
        """Parse JSON transactions from Kafka"""
        # Parse the JSON value from Kafka
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.transaction_schema).alias("transaction"),
            col("timestamp").alias("kafka_timestamp")
        ).select("transaction.*", "kafka_timestamp")
        
        # Convert timestamp string to actual timestamp
        parsed_df = parsed_df.withColumn(
            "transaction_timestamp",
            col("timestamp").cast(TimestampType())
        )
        
        # Extract lat/long from location struct
        parsed_df = parsed_df.withColumn("latitude", col("location.lat"))
        parsed_df = parsed_df.withColumn("longitude", col("location.long"))
        
        return parsed_df
    
    def apply_fraud_rules(self, df):
        """Apply fraud detection rules to transactions"""
        # Register distance UDF
        distance_udf = self.register_distance_udf()
        
        # Calculate distance from Nairobi
        df_with_distance = df.withColumn(
            "distance_from_nairobi",
            distance_udf(col("latitude"), col("longitude"))
        )
        
        # Apply fraud detection rules
        df_with_fraud = df_with_distance.withColumn(
            "is_high_amount",
            when(col("amount") > 10000, True).otherwise(False)
        ).withColumn(
            "is_location_outlier",
            when(col("distance_from_nairobi") > self.distance_threshold_km, True).otherwise(False)
        )
        
        # Flagged transactions (any fraud rule triggered)
        df_with_fraud = df_with_fraud.withColumn(
            "is_flagged",
            (col("is_high_amount") | col("is_location_outlier")).cast("boolean")
        )
        
        return df_with_fraud
    
    def detect_user_frequency_fraud(self, df):
        """Detect users with >5 transactions in the window"""
        # Count transactions per user in the window
        user_counts = df.groupBy(
            window(col("transaction_timestamp"), self.window_duration, self.slide_duration),
            col("user_id")
        ).agg(
            count("*").alias("transaction_count")
        )
        
        # Flag users with >5 transactions
        user_fraud = user_counts.withColumn(
            "is_high_frequency",
            when(col("transaction_count") > 5, True).otherwise(False)
        ).filter(col("is_high_frequency") == True)
        
        # Extract window information
        user_fraud = user_fraud.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_id"),
            col("transaction_count"),
            col("is_high_frequency")
        )
        
        return user_fraud
    
    def aggregate_metrics(self, df):
        """Aggregate transaction metrics by hour"""
        # Add hour column
        df_with_hour = df.withColumn("hour", hour(col("transaction_timestamp")))
        
        # Aggregate by hour
        hourly_metrics = df_with_hour.groupBy("hour").agg(
            count("*").alias("total_transactions"),
            spark_sum(col("amount")).alias("total_volume"),
            sum(col("is_flagged").cast("int")).alias("flagged_count"),
            avg(col("amount")).alias("avg_amount"),
            max(col("amount")).alias("max_amount"),
            min(col("amount")).alias("min_amount")
        ).orderBy("hour")
        
        return hourly_metrics
    
    def create_console_queries(self, parsed_stream):
        """Create streaming queries for console output"""
        # Apply fraud rules
        fraud_stream = self.apply_fraud_rules(parsed_stream)
        
        # Detect user frequency fraud
        user_fraud_stream = self.detect_user_frequency_fraud(fraud_stream)
        
        # Aggregate metrics
        metrics_stream = self.aggregate_metrics(fraud_stream)
        
        # Query 1: Flagged transactions
        flagged_query = fraud_stream.filter(col("is_flagged") == True) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "100") \
            .start()
        
        # Query 2: User frequency fraud
        user_fraud_query = user_fraud_stream \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        # Query 3: Hourly metrics
        metrics_query = metrics_stream \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='60 seconds') \
            .start()
        
        return [flagged_query, user_fraud_query, metrics_query]
    
    def create_elasticsearch_sink(self, df, document_type: str):
        """Create a foreachBatch sink to write data to Elasticsearch"""
        def write_to_elasticsearch(batch_df, batch_id):
            try:
                # Convert DataFrame to list of dictionaries
                rows = batch_df.collect()
                
                if not rows:
                    return
                
                # Prepare documents for Elasticsearch
                documents = []
                for row in rows:
                    doc = row.asDict()
                    doc['document_type'] = document_type
                    
                    # Convert location to geo_point format
                    if 'latitude' in doc and 'longitude' in doc:
                        doc['location'] = {
                            'lat': doc['latitude'],
                            'lon': doc['longitude']
                        }
                    
                    # Convert timestamp to ISO format
                    if 'transaction_timestamp' in doc and doc['transaction_timestamp']:
                        doc['transaction_timestamp'] = doc['transaction_timestamp'].isoformat()
                    
                    documents.append(doc)
                
                # Bulk index to Elasticsearch
                if documents:
                    self.bulk_index_documents(documents)
                    print(f"Indexed {len(documents)} {document_type} documents to Elasticsearch")
                    
            except Exception as e:
                print(f"Error writing batch {batch_id} to Elasticsearch: {e}")
        
        return write_to_elasticsearch
    
    def bulk_index_documents(self, documents: List[Dict]):
        """Bulk index documents to Elasticsearch with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Prepare bulk actions
                actions = []
                for doc in documents:
                    actions.append({
                        "_index": self.elasticsearch_index,
                        "_source": doc
                    })
                
                # Execute bulk request
                from elasticsearch.helpers import bulk
                success, failed = bulk(self.es, actions)
                
                if failed:
                    print(f"Failed to index {len(failed)} documents")
                
                return
                
            except ConnectionError as e:
                if attempt < max_retries - 1:
                    print(f"Elasticsearch connection error (attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    raise
            except Exception as e:
                print(f"Error indexing documents: {e}")
                raise
    
    def create_elasticsearch_queries(self, parsed_stream):
        """Create streaming queries for Elasticsearch output using elasticsearch-py"""
        # Apply fraud rules
        fraud_stream = self.apply_fraud_rules(parsed_stream)
        
        # Query 1: Flagged transactions to Elasticsearch
        flagged_sink = self.create_elasticsearch_sink(fraud_stream, "flagged_transaction")
        
        flagged_query = fraud_stream.filter(col("is_flagged") == True) \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(flagged_sink) \
            .option("checkpointLocation", f"{self.checkpoint_location}/es_flagged") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        # Query 2: Hourly aggregates to Elasticsearch
        metrics_stream = self.aggregate_metrics(fraud_stream)
        
        def format_hourly_metrics(batch_df, batch_id):
            try:
                rows = batch_df.collect()
                
                for row in rows:
                    doc = {
                        'hour': str(row.hour).zfill(2),
                        'total_volume': float(row.total_volume),
                        'flagged_count': int(row.flagged_count),
                        'total_transactions': int(row.total_transactions),
                        'avg_amount': float(row.avg_amount),
                        'max_amount': float(row.max_amount),
                        'min_amount': float(row.min_amount),
                        'document_type': 'hourly_aggregate',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # Index to Elasticsearch
                    try:
                        self.es.index(
                            index=self.elasticsearch_index,
                            body=doc
                        )
                    except Exception as e:
                        print(f"Error indexing hourly aggregate: {e}")
                        
                if rows:
                    print(f"Indexed {len(rows)} hourly aggregates to Elasticsearch")
                    
            except Exception as e:
                print(f"Error processing hourly metrics batch {batch_id}: {e}")
        
        metrics_query = metrics_stream \
            .writeStream \
            .outputMode("complete") \
            .foreachBatch(format_hourly_metrics) \
            .option("checkpointLocation", f"{self.checkpoint_location}/es_metrics") \
            .trigger(processingTime='60 seconds') \
            .start()
        
        return [flagged_query, metrics_query]
    
    def start_streaming(self, output_mode: str = "console"):
        """Start the streaming processor"""
        print(f"Starting M-Pesa stream processor...")
        print(f"Kafka servers: {self.kafka_bootstrap_servers}")
        print(f"Topic: {self.kafka_topic}")
        print(f"Window: {self.window_duration}")
        print(f"Output mode: {output_mode}")
        
        try:
            # Read from Kafka
            kafka_stream = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse transactions
            parsed_stream = self.parse_transactions(kafka_stream)
            
            # Create queries based on output mode
            if output_mode == "console":
                queries = self.create_console_queries(parsed_stream)
            elif output_mode == "elasticsearch":
                queries = self.create_elasticsearch_queries(parsed_stream)
            else:
                queries = self.create_console_queries(parsed_stream)
            
            print(f"Started {len(queries)} streaming queries")
            print("Stream processor is running. Press Ctrl+C to stop.")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            print(f"Error in streaming processor: {e}")
            raise
        finally:
            print("Stopping stream processor...")
    
    def stop(self):
        """Stop the streaming processor and Elasticsearch client"""
        if self.spark:
            self.spark.stop()
            print("Spark session stopped")
        
        if self.es:
            self.es.close()
            print("Elasticsearch client closed")


def main():
    """Main function to run the stream processor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='M-Pesa Stream Processor')
    parser.add_argument('--kafka-servers', type=str, default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='mpesa-transactions',
                       help='Kafka topic name')
    parser.add_argument('--window', type=str, default='60 seconds',
                       help='Window duration')
    parser.add_argument('--slide', type=str, default='30 seconds',
                       help='Slide duration')
    parser.add_argument('--output', choices=['console', 'elasticsearch'], 
                       default='console', help='Output mode')
    parser.add_argument('--checkpoint', type=str, default='data/checkpoints',
                       help='Checkpoint location')
    
    parser.add_argument('--elasticsearch-host', type=str, default='localhost',
                       help='Elasticsearch host')
    parser.add_argument('--elasticsearch-port', type=int, default=9200,
                       help='Elasticsearch port')
    parser.add_argument('--elasticsearch-index', type=str, default='mpesa-insights',
                       help='Elasticsearch index name')
    
    args = parser.parse_args()
    
    # Create processor
    processor = MPesaStreamProcessor(
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_topic=args.topic,
        window_duration=args.window,
        slide_duration=args.slide,
        checkpoint_location=args.checkpoint,
        elasticsearch_host=args.elasticsearch_host,
        elasticsearch_port=args.elasticsearch_port,
        elasticsearch_index=args.elasticsearch_index
    )
    
    try:
        processor.start_streaming(output_mode=args.output)
    except KeyboardInterrupt:
        print("\nStream processor stopped by user")
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
