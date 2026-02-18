#!/usr/bin/env python3
"""
Spark Structured Streaming performance test
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import psutil
import json
from datetime import datetime

class SparkStreamingPerformanceTest:
    def __init__(self, master_url='spark://spark-master:7077'):
        self.spark = SparkSession.builder \
            .appName("StreamingPerformanceTest") \
            .master(master_url) \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.streaming.backpressure.initialRate", "1000") \
            .config("spark.streaming.kafka.maxRatePerPartition", "2000") \
            .getOrCreate()
    
    def test_kafka_throughput(self, bootstrap_servers='kafka:9092', topic='stock_prices', duration_seconds=60):
        """Measure Spark streaming throughput from Kafka"""
        
        # Read stream from Kafka
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse and transform
        parsed_df = df.select(
            col("value").cast("string"),
            col("timestamp")
        )
        
        # Count messages per batch
        count_df = parsed_df.groupBy(
            window(col("timestamp"), "10 seconds")
        ).count()
        
        # Memory stream for collecting metrics
        query = count_df.writeStream \
            .outputMode("complete") \
            .format("memory") \
            .queryName("throughput_metrics") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        # Collect metrics
        start_time = time.time()
        metrics = []
        
        while time.time() - start_time < duration_seconds:
            time.sleep(5)
            
            # Query current throughput
            result = self.spark.sql("""
                SELECT 
                    window.end as batch_time,
                    count as records_processed
                FROM throughput_metrics
                ORDER BY window.end DESC
                LIMIT 1
            """).collect()
            
            if result:
                metrics.append({
                    'timestamp': result[0].batch_time.isoformat(),
                    'records_per_second': result[0].records_processed / 10  # 10-second window
                })
        
        query.stop()
        
        # Calculate statistics
        if metrics:
            throughputs = [m['records_per_second'] for m in metrics]
            avg_throughput = sum(throughputs) / len(throughputs)
            peak_throughput = max(throughputs)
        else:
            avg_throughput = peak_throughput = 0
        
        return {
            'avg_throughput': avg_throughput,
            'peak_throughput': peak_throughput,
            'samples': metrics
        }
    
    def test_processing_latency(self):
        """Measure Spark processing latency"""
        
        # Create test streaming query
        test_df = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", "1000") \
            .load()
        
        # Complex transformation to measure latency
        transformed_df = test_df \
            .withColumn("value_doubled", col("value") * 2) \
            .withColumn("value_squared", col("value") ** 2) \
            .withColumn("processing_time", current_timestamp())
        
        # Measure latency
        latency_query = transformed_df.writeStream \
            .format("memory") \
            .queryName("latency_test") \
            .trigger(processingTime="1 second") \
            .start()
        
        time.sleep(10)
        
        # Check processing latency
        latency_df = self.spark.sql("""
            SELECT 
                timestamp,
                processing_time,
                (unix_timestamp(processing_time) - unix_timestamp(timestamp)) * 1000 as latency_ms
            FROM latency_test
            ORDER BY timestamp DESC
            LIMIT 100
        """)
        
        latencies = [row.latency_ms for row in latency_df.collect()]
        
        latency_query.stop()
        
        return {
            'avg_latency_ms': sum(latencies) / len(latencies) if latencies else 0,
            'max_latency_ms': max(latencies) if latencies else 0,
            'min_latency_ms': min(latencies) if latencies else 0
        }
    
    def cleanup(self):
        self.spark.stop()

def main():
    test = SparkStreamingPerformanceTest()
    
    print("="*50)
    print("SPARK STREAMING PERFORMANCE TEST")
    print("="*50)
    
    # Test throughput
    print("\n📊 Testing Kafka throughput...")
    throughput_results = test.test_kafka_throughput(duration_seconds=30)
    print(f"Average throughput: {throughput_results['avg_throughput']:.0f} records/sec")
    print(f"Peak throughput: {throughput_results['peak_throughput']:.0f} records/sec")
    
    # Test latency
    print("\n⏱️  Testing processing latency...")
    latency_results = test.test_processing_latency()
    print(f"Average latency: {latency_results['avg_latency_ms']:.2f} ms")
    print(f"Max latency: {latency_results['max_latency_ms']:.2f} ms")
    
    test.cleanup()
    
    # Save results
    results = {
        'timestamp': datetime.now().isoformat(),
        'spark_throughput': throughput_results,
        'spark_latency': latency_results
    }
    
    with open('spark_performance_report.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()