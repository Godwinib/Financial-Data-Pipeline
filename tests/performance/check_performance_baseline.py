#!/usr/bin/env python3
"""
Check performance against baselines
"""
import json
import os

BASELINES = {
    'kafka_throughput': 5000,  # messages/second
    'kafka_p95_latency': 50,   # ms
    'spark_throughput': 2000,  # records/second
    'spark_latency': 100,      # ms
    'postgres_inserts': 1000,  # records/second
    'postgres_query': 50       # ms
}

def check_performance():
    failures = []
    
    # Check Kafka performance
    if os.path.exists('performance_report.json'):
        with open('performance_report.json') as f:
            kafka_results = json.load(f)
            
        throughput = kafka_results['producer']['throughput_messages_per_second']
        if throughput < BASELINES['kafka_throughput']:
            failures.append(f"Kafka throughput: {throughput:.0f} < {BASELINES['kafka_throughput']}")
        
        latency = kafka_results['latency']['p95_latency_ms']
        if latency > BASELINES['kafka_p95_latency']:
            failures.append(f"Kafka p95 latency: {latency:.2f} > {BASELINES['kafka_p95_latency']}")
    
    # Check Spark performance
    if os.path.exists('spark_performance_report.json'):
        with open('spark_performance_report.json') as f:
            spark_results = json.load(f)
        
        throughput = spark_results['spark_throughput']['avg_throughput']
        if throughput < BASELINES['spark_throughput']:
            failures.append(f"Spark throughput: {throughput:.0f} < {BASELINES['spark_throughput']}")
        
        latency = spark_results['spark_latency']['avg_latency_ms']
        if latency > BASELINES['spark_latency']:
            failures.append(f"Spark latency: {latency:.2f} > {BASELINES['spark_latency']}")
    
    # Check PostgreSQL performance
    if os.path.exists('postgres_performance_report.json'):
        with open('postgres_performance_report.json') as f:
            pg_results = json.load(f)
        
        insert_rate = pg_results['insert_performance']['batch_100']['records_per_second']
        if insert_rate < BASELINES['postgres_inserts']:
            failures.append(f"PostgreSQL inserts: {insert_rate:.0f} < {BASELINES['postgres_inserts']}")
        
        query_time = pg_results['query_performance']['simple_select']['avg_execution_time_ms']
        if query_time > BASELINES['postgres_query']:
            failures.append(f"PostgreSQL query: {query_time:.2f} > {BASELINES['postgres_query']}")
    
    if failures:
        print("❌ Performance regressions detected:")
        for failure in failures:
            print(f"  - {failure}")
        exit(1)
    else:
        print("✅ All performance metrics within baselines")

if __name__ == "__main__":
    check_performance()