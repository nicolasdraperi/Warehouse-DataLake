# 🌍 Weather Data Warehouse Pipeline

## Stack
- Kafka (streaming)
- Spark Structured Streaming
- HDFS (historical storage)
- Node.js (API)
- HTML/CSS/JS + Chart.js (dashboard)

## Architecture
Weather API → Kafka → Spark → Kafka/HDFS → Dashboard

## Features
- Real-time weather ingestion
- Streaming transformations & alerts
- Historical aggregation (10 years)
- Seasonal profiles & anomaly detection
- Global visualization dashboard

## How to run
```bash
docker-compose up -d
node work/frontend/server.js
