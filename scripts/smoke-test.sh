#!/bin/bash
set -e

echo "Running smoke tests..."

# Test Kafka
kafka_ready=$(docker-compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list | grep stock_prices || echo "")
if [ -z "$kafka_ready" ]; then
    echo "❌ Kafka not ready"
    exit 1
fi
echo "✅ Kafka ready"

# Test PostgreSQL
pg_ready=$(docker-compose exec -T postgres pg_isready -U stockuser)
if [ $? -ne 0 ]; then
    echo "❌ PostgreSQL not ready"
    exit 1
fi
echo "✅ PostgreSQL ready"

# Test Spark
spark_ready=$(curl -s http://localhost:8081 | grep -i "Alive" || echo "")
if [ -z "$spark_ready" ]; then
    echo "❌ Spark not ready"
    exit 1
fi
echo "✅ Spark ready"

echo "All smoke tests passed! ✅"