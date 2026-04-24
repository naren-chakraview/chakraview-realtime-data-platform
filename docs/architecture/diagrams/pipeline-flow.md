---
title: Pipeline Flow
description: End-to-end data movement from PostgreSQL WAL commit to analyst-queryable Gold Iceberg table.
tags: [architecture, diagrams]
---

# Pipeline Flow

```mermaid
flowchart TD
    subgraph Source["Source Systems"]
        PG_O["PostgreSQL\norders.orders\norders.order_items"]
        PG_I["PostgreSQL\ninventory.stock_items"]
        PG_C["PostgreSQL\ncustomers.customers"]
    end

    subgraph CDC["CDC Layer"]
        DBZ_O["Debezium\norders-source-connector"]
        DBZ_I["Debezium\ninventory-source-connector"]
    end

    subgraph Bus["Message Bus"]
        KF_O["Kafka\nchakra.orders.placed\nchakra.orders.items"]
        KF_I["Kafka\nchakra.inventory.updated"]
        SR["Schema Registry\nBACKWARD_TRANSITIVE"]
        DLQ_K["Kafka DLQ\nchakra.orders.dlq"]
    end

    subgraph Processing["Flink Stream Processing"]
        FL_B["Bronze Sink\nappend raw CDC"]
        FL_V["ValidateAndDeduplicate\nRocksDB · 24h TTL"]
        FL_S["Silver Sink\nenvelope unwrapped"]
        FL_D["DLQ Side Output\nfailure_stage · failure_reason"]
    end

    subgraph Lake["Lakehouse (S3 + Iceberg)"]
        BRONZE["Bronze Tables\nraw CDC · 90-day snapshots\n≤ 30s SLA"]
        SILVER["Silver Tables\nvalidated · deduped · 1-year\n≤ 2 min SLA"]
        GOLD["Gold Tables\nbusiness aggregations · 2-year\n≤ 5 min SLA"]
        DLQ_I["DLQ Tables\nfailure audit · Iceberg"]
    end

    subgraph Serving["Serving"]
        DBT["dbt\nincremental Gold models"]
        DUCK["DuckDB\niceberg_scan()"]
        ATH["AWS Athena\n(production scale)"]
    end

    PG_O -->|"WAL"| DBZ_O
    PG_I -->|"WAL"| DBZ_I
    PG_C -.->|"future"| CDC

    DBZ_O -->|"Avro · < 1s"| KF_O
    DBZ_I -->|"Avro · < 1s"| KF_I

    KF_O --- SR
    KF_I --- SR
    DBZ_O -->|"schema errors"| DLQ_K

    KF_O -->|"COMMITTED isolation"| FL_B
    KF_O --> FL_V
    FL_B --> BRONZE
    FL_V -->|"valid"| FL_S
    FL_V -->|"invalid"| FL_D
    FL_S --> SILVER
    FL_D --> DLQ_I

    SILVER --> DBT --> GOLD
    GOLD --> DUCK
    GOLD -.->|"> 10 concurrent\n> 50 GB scan"| ATH

    style BRONZE fill:#92400e,color:#fff
    style SILVER fill:#1e3a5f,color:#fff
    style GOLD fill:#14532d,color:#fff
    style DLQ_I fill:#7f1d1d,color:#fff
    style DLQ_K fill:#7f1d1d,color:#fff
    style DUCK fill:#0f766e,color:#fff
    style ATH fill:#374151,color:#fff
```

## SLA Checkpoints

Each arrow with a latency annotation is a machine-readable SLA defined in `contracts/data-products/orders-analytics.yaml`. A breach fires a Prometheus burn-rate alert that pages the `owner_team` defined in the same contract.

| Checkpoint | SLA | Metric |
|---|---|---|
| WAL → Kafka | < 1 second | Debezium connector lag |
| Kafka → Bronze Iceberg | ≤ 30 seconds | `orders_bronze_lag_seconds` |
| Bronze → Silver | ≤ 2 minutes | Kafka consumer lag (`flink-silver-orders` group) |
| Silver → Gold | ≤ 5 minutes | `orders_gold_last_updated_timestamp_seconds` |
