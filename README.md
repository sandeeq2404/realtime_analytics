# 🍔 FoodStream Analytics

A real-time food order analytics pipeline processing streaming data from generation to sub-second querying, orchestrated entirely in a single `docker-compose up`. 

**Tech Stack:** Python (Faker), Apache Kafka, PySpark (Structured Streaming), Apache Pinot, Docker.

## Architecture

┌─────────────────────────────────────────────────────────────────────┐
│                      FoodStream Analytics Pipeline                  │
│                                                                     │
│  ┌──────────────┐     ┌─────────────────┐     ┌─────────────────┐   │
│  │   Python     │     │  Apache Kafka   │     │  Apache Pinot   │   │
│  │  Producer    │────▶│                 │────▶│  (REALTIME      │   │
│  │              │     │  raw-food-orders│     │   Table)        │   │
│  │  Generates   │     │       ↓         │     │                 │   │
│  │  ~1 order/s  │     │  PySpark Stream │     │  SQL Queries    │   │
│  │  via Faker   │     │  Processor      │     │  in < 1 second  │   │
│  └──────────────┘     │       ↓         │     └─────────────────┘   │
│                       │  processed-food-│           ▲               │
│                       │  orders         │───────────┘               │
│                       └─────────────────┘                           │
│       [Kafka UI]                              [Pinot UI]            │
│     localhost:8888                           localhost:9000         │
└─────────────────────────────────────────────────────────────────────┘

1. **Generate:** Python script pushes Faker-generated Indian food orders to `raw-food-orders` Kafka topic.
2. **Process:** PySpark Structured Streaming reads, enriches (computes peak hours, value categories), and forwards to `processed-food-orders`.
3. **Ingest & Query:** Apache Pinot auto-consumes the enriched stream via a REALTIME table, allowing sub-second SQL analytics on live data.

