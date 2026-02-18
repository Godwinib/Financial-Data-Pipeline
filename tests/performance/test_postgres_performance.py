#!/usr/bin/env python3
"""
PostgreSQL performance test for financial data pipeline
"""
import psycopg2
import psycopg2.extras
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresPerformanceTest:
    def __init__(self, host='localhost', port=5432, database='stockdb', 
                 user='stockuser', password='stockpass'):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
    
    def test_insert_performance(self, num_records=10000, batch_size=100):
        """Test insert performance with different batch sizes"""
        
        results = {}
        
        for batch in [10, 50, 100, 500, 1000]:
            conn = psycopg2.connect(**self.connection_params)
            cur = conn.cursor()
            
            # Clear existing test data
            cur.execute("DELETE FROM fact_stock_prices WHERE symbol = 'PERF'")
            conn.commit()
            
            start_time = time.time()
            inserted = 0
            
            for i in range(0, num_records, batch):
                records = []
                for j in range(min(batch, num_records - i)):
                    records.append((
                        f"PERF_{i+j}",
                        'PERF',
                        100.00 + (j * 0.01),
                        1000000 + j,
                        110.00,
                        90.00,
                        0.50,
                        0.5,
                        datetime.now(),
                        datetime.now(),
                        datetime.now()
                    ))
                
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fact_stock_prices 
                    (id, symbol, price, volume, day_high, day_low, change, 
                     change_percent, event_time, processing_time, load_time)
                    VALUES %s
                    """,
                    records
                )
                inserted += len(records)
            
            conn.commit()
            end_time = time.time()
            
            total_time = end_time - start_time
            throughput = num_records / total_time
            
            results[f'batch_{batch}'] = {
                'batch_size': batch,
                'total_time_seconds': total_time,
                'records_per_second': throughput,
                'total_records': inserted
            }
            
            cur.close()
            conn.close()
        
        return results
    
    def test_query_performance(self):
        """Test query performance with different query patterns"""
        
        conn = psycopg2.connect(**self.connection_params)
        cur = conn.cursor()
        
        queries = {
            'simple_select': "SELECT * FROM fact_stock_prices LIMIT 1000",
            'aggregate_by_symbol': """
                SELECT symbol, AVG(price), SUM(volume) 
                FROM fact_stock_prices 
                GROUP BY symbol
            """,
            'time_range': """
                SELECT * FROM fact_stock_prices 
                WHERE event_time >= NOW() - INTERVAL '1 hour'
            """,
            'join_with_dim': """
                SELECT f.*, s.company_name 
                FROM fact_stock_prices f
                JOIN dim_stock s ON f.symbol = s.symbol
                WHERE f.event_time >= NOW() - INTERVAL '1 day'
            """,
            'materialized_view': """
                SELECT * FROM mv_daily_stock_summary 
                WHERE trade_date = CURRENT_DATE
            """
        }
        
        results = {}
        
        for query_name, query in queries.items():
            timings = []
            
            for _ in range(5):  # Run 5 times
                start_time = time.time()
                cur.execute(query)
                cur.fetchall()
                end_time = time.time()
                timings.append((end_time - start_time) * 1000)  # ms
            
            results[query_name] = {
                'avg_execution_time_ms': statistics.mean(timings),
                'min_execution_time_ms': min(timings),
                'max_execution_time_ms': max(timings)
            }
        
        cur.close()
        conn.close()
        
        return results
    
    def test_concurrent_connections(self, num_connections=10, queries_per_connection=5):
        """Test performance under concurrent load"""
        
        def execute_queries(connection_id):
            conn = psycopg2.connect(**self.connection_params)
            cur = conn.cursor()
            timings = []
            
            for i in range(queries_per_connection):
                start = time.time()
                cur.execute("SELECT * FROM fact_stock_prices LIMIT 100")
                cur.fetchall()
                timings.append((time.time() - start) * 1000)
            
            cur.close()
            conn.close()
            
            return {
                'connection_id': connection_id,
                'avg_query_time': statistics.mean(timings),
                'total_time': sum(timings)
            }
        
        with ThreadPoolExecutor(max_workers=num_connections) as executor:
            futures = [executor.submit(execute_queries, i) for i in range(num_connections)]
            
            results = []
            for future in as_completed(futures):
                results.append(future.result())
        
        return {
            'num_connections': num_connections,
            'queries_per_connection': queries_per_connection,
            'avg_response_time_ms': statistics.mean([r['avg_query_time'] for r in results]),
            'total_execution_time_ms': sum([r['total_time'] for r in results])
        }

def main():
    test = PostgresPerformanceTest()
    
    print("="*50)
    print("POSTGRESQL PERFORMANCE TEST")
    print("="*50)
    
    # Test insert performance
    print("\n📝 Testing INSERT performance...")
    insert_results = test.test_insert_performance(num_records=5000)
    for batch_config, metrics in insert_results.items():
        print(f"Batch size {metrics['batch_size']:4d}: {metrics['records_per_second']:.0f} records/sec")
    
    # Test query performance
    print("\n🔍 Testing query performance...")
    query_results = test.test_query_performance()
    for query_name, metrics in query_results.items():
        print(f"{query_name:25s}: {metrics['avg_execution_time_ms']:.2f} ms avg")
    
    # Test concurrent connections
    print("\n👥 Testing concurrent connections...")
    concurrent_results = test.test_concurrent_connections(num_connections=10)
    print(f"10 concurrent connections: {concurrent_results['avg_response_time_ms']:.2f} ms avg response")
    
    # Save results
    results = {
        'timestamp': datetime.now().isoformat(),
        'insert_performance': insert_results,
        'query_performance': query_results,
        'concurrent_performance': concurrent_results
    }
    
    with open('postgres_performance_report.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()