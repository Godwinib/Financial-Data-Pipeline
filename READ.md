# Real-Time Financial Data Pipeline

A production-ready real-time financial data pipeline using Apache Kafka, Spark Structured Streaming, PostgreSQL, and modern monitoring tools.

## Architecture

- **Data Source**: RapidAPI Real-Time Finance Data
- **Message Broker**: Apache Kafka (KRaft mode)
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage**: PostgreSQL (Star Schema)
- **Monitoring**: Prometheus + Grafana
- **Visualization**: Power BI / PgAdmin

## Prerequisites

- Docker and Docker Compose
- RapidAPI Key (from [RapidAPI](https://rapidapi.com/))
- 8GB RAM minimum (16GB recommended)

## Quick Start

1. Clone the repository:
```bash
git clone <repository-url>
cd financial-data-pipeline