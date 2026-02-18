#!/usr/bin/env python3
"""
Kafka throughput and latency performance test using Python
"""
import json
import time
import threading
import statistics
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaPerformanceTest:
    def __init__(self, bootstrap_servers='localhost:9092', topic='perf_test'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.latencies = []
        self.throughput = []
        
    def create_test_topic(self):
        """Create test topic with appropriate configuration"""
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='perf_test_admin'
        )
        
        topic = NewTopic(
            name=self.topic,
            num_partitions=3,
            replication_factor=1
        )
        
        try:
            admin_client.create_topics([topic])
            logger.info(f"Created topic: {self.topic}")
        except TopicAlreadyExistsError:
            logger.info(f"Topic {self.topic} already exists")
        finally:
            admin_client.close()
    
    def producer_performance_test(self, message_count=10000, message_size=1024):
        """Test producer throughput and latency"""
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='snappy',
            batch_size=16384,
            linger_ms=10,
            acks='all'
        )
        
        # Generate test data
        test_data = {
            'symbol': 'PERF',
            'price': 100.00,
            'volume': 10000,
            'event_time': datetime.now().isoformat(),
            'payload': 'x' * message_size
        }
        
        start_time = time.time()
        latencies = []
        
        for i in range(message_count):
            send_start = time.time()
            future = producer.send(self.topic, key=f'key-{i}'.encode(), value=test_data)
            future.get(timeout=10)
            latency = (time.time() - send_start) * 1000  # ms
            latencies.append(latency)
            
            if i % 1000 == 0:
                logger.info(f"Sent {i} messages")
        
        producer.flush()
        end_time = time.time()
        producer.close()
        
        total_time = end_time - start_time
        throughput = message_count / total_time
        
        return {
            'total_messages': message_count,
            'total_time_seconds': total_time,
            'throughput_messages_per_second': throughput,
            'throughput_mb_per_second': (throughput * message_size) / (1024 * 1024),
            'avg_latency_ms': statistics.mean(latencies),
            'p95_latency_ms': np.percentile(latencies, 95),
            'p99_latency_ms': np.percentile(latencies, 99),
            'max_latency_ms': max(latencies),
            'min_latency_ms': min(latencies)
        }
    
    def consumer_performance_test(self, duration_seconds=30):
        """Test consumer throughput and lag"""
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='perf_test_consumer',
            max_poll_records=500
        )
        
        message_count = 0
        start_time = time.time()
        throughput_samples = []
        
        while time.time() - start_time < duration_seconds:
            batch_start = time.time()
            records = consumer.poll(timeout_ms=1000)
            
            for tp, messages in records.items():
                message_count += len(messages)
            
            batch_time = time.time() - batch_start
            if records:
                throughput = sum(len(messages) for messages in records.values()) / batch_time
                throughput_samples.append(throughput)
            
            if message_count % 1000 == 0:
                logger.info(f"Consumed {message_count} messages")
        
        consumer.close()
        
        return {
            'total_messages_consumed': message_count,
            'duration_seconds': duration_seconds,
            'avg_throughput': statistics.mean(throughput_samples) if throughput_samples else 0,
            'peak_throughput': max(throughput_samples) if throughput_samples else 0
        }
    
    def end_to_end_latency_test(self, message_count=1000):
        """Measure end-to-end latency from produce to consume"""
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            group_id='latency_test'
        )
        
        # Ensure consumer is ready
        consumer.poll(timeout_ms=1000)
        consumer.seek_to_end()
        
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        latencies = []
        
        for i in range(message_count):
            # Create message with timestamp
            send_time = time.time() * 1000  # ms
            message = {
                'id': i,
                'send_timestamp': send_time,
                'data': f'test_message_{i}'
            }
            
            # Send message
            future = producer.send(self.topic, key=f'latency-{i}'.encode(), value=message)
            future.get(timeout=10)
            
            # Try to consume the same message
            records = consumer.poll(timeout_ms=5000)
            
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        value = json.loads(msg.value.decode('utf-8'))
                        if value.get('id') == i:
                            receive_time = time.time() * 1000
                            latency = receive_time - value['send_timestamp']
                            latencies.append(latency)
                    except:
                        pass
        
        producer.close()
        consumer.close()
        
        return {
            'sample_size': len(latencies),
            'avg_latency_ms': statistics.mean(latencies) if latencies else 0,
            'p95_latency_ms': np.percentile(latencies, 95) if latencies else 0,
            'p99_latency_ms': np.percentile(latencies, 99) if latencies else 0,
            'max_latency_ms': max(latencies) if latencies else 0
        }

def main():
    parser = argparse.ArgumentParser(description='Kafka Performance Testing')
    parser.add_argument('--brokers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--messages', type=int, default=10000, help='Number of messages')
    parser.add_argument('--size', type=int, default=1024, help='Message size in bytes')
    parser.add_argument('--duration', type=int, default=30, help='Test duration in seconds')
    
    args = parser.parse_args()
    
    test = KafkaPerformanceTest(bootstrap_servers=args.brokers)
    
    # Create test topic
    test.create_test_topic()
    
    # Run producer performance test
    logger.info("Running producer performance test...")
    producer_results = test.producer_performance_test(
        message_count=args.messages,
        message_size=args.size
    )
    
    print("\n" + "="*50)
    print("PRODUCER PERFORMANCE RESULTS")
    print("="*50)
    for key, value in producer_results.items():
        print(f"{key:30}: {value:.2f}")
    
    # Run consumer performance test
    logger.info("\nRunning consumer performance test...")
    consumer_results = test.consumer_performance_test(duration_seconds=args.duration)
    
    print("\n" + "="*50)
    print("CONSUMER PERFORMANCE RESULTS")
    print("="*50)
    for key, value in consumer_results.items():
        if isinstance(value, float):
            print(f"{key:30}: {value:.2f}")
        else:
            print(f"{key:30}: {value}")
    
    # Run end-to-end latency test
    logger.info("\nRunning end-to-end latency test...")
    latency_results = test.end_to_end_latency_test(message_count=1000)
    
    print("\n" + "="*50)
    print("END-TO-END LATENCY RESULTS")
    print("="*50)
    for key, value in latency_results.items():
        if isinstance(value, float):
            print(f"{key:20}: {value:.2f} ms")
        else:
            print(f"{key:20}: {value}")
    
    # Generate performance report
    report = {
        'timestamp': datetime.now().isoformat(),
        'test_config': {
            'brokers': args.brokers,
            'message_count': args.messages,
            'message_size_bytes': args.size,
            'test_duration': args.duration
        },
        'producer': producer_results,
        'consumer': consumer_results,
        'latency': latency_results
    }
    
    with open('performance_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info("\nPerformance report saved to performance_report.json")

if __name__ == "__main__":
    main()