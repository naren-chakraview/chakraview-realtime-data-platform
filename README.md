# chakraview-realtime-data-platform

> A reference architecture for a contract-first real-time data platform — where domain teams author data product SLAs and AI agents implement the pipeline topology from those contracts.

---

## What This Is

This repo documents a real-time analytics platform built on top of the [Chakra Commerce](https://github.com/naren-chakraview/chakraview-enterprise-modernization) transactional system. Order events, inventory changes, and customer registrations flow from PostgreSQL WALs through a streaming pipeline into a lakehouse, where analysts query Gold-layer tables with sub-second latency.

The architectural thesis mirrors the enterprise modernization repo: **humans author contracts; agents build from them**.

Domain teams author **data product contracts** — Avro schemas, freshness SLAs, quality invariants, and Gold-layer table definitions. The pipeline topology (Debezium connector configs, Flink jobs, Iceberg table DDL, dbt models) is derived from those contracts by AI agents. The data product contract is the only source of truth the analytics platform ever needs to trust.

---

## Architecture

```
PostgreSQL WAL (Orders, Inventory, Customers)
    │
    │  Debezium CDC (WAL-based, sub-second latency)
    ▼
Kafka + Schema Registry (Avro, BACKWARD_TRANSITIVE compatibility)
    │
    ├─ Kafka Streams ─► lightweight filter / route / enrich
    │
    │  Apache Flink (PyFlink, exactly-once, RocksDB state)
    ▼
┌───────────────────────────────────────────────┐
│  Apache Iceberg on S3 (Medallion layers)      │
│                                               │
│  Bronze  raw CDC events      90-day snapshots │
│  Silver  validated + deduped  1-year snapshots │
│  Gold    aggregated / serving 2-year snapshots │
└───────────────────────────────────────────────┘
    │
    ├─ DuckDB ──► analysts, notebooks, data quality scripts
    │              (local dev: zero infrastructure)
    │
    └─ AWS Athena ► BI tools, high-concurrency dashboards
                    (production scale — see ADR-0005)
```

---

## Architecture Decisions

| ADR | Decision | Status |
|---|---|---|
| [ADR-0001](docs/adrs/ADR-0001-kappa-architecture.md) | Kappa Architecture over Lambda | Accepted |
| [ADR-0002](docs/adrs/ADR-0002-medallion-architecture.md) | Medallion Architecture (Bronze / Silver / Gold) | Accepted |
| [ADR-0003](docs/adrs/ADR-0003-debezium-cdc.md) | Debezium for Change Data Capture | Accepted |
| [ADR-0004](docs/adrs/ADR-0004-apache-iceberg.md) | Apache Iceberg for Lakehouse Storage | Accepted |
| [ADR-0005](docs/adrs/ADR-0005-duckdb-serving-layer.md) | DuckDB as Serving Layer (Athena: production-scale path) | Accepted |
| [ADR-0006](docs/adrs/ADR-0006-avro-schema-registry.md) | Avro + Schema Registry (BACKWARD_TRANSITIVE) | Accepted |
| [ADR-0007](docs/adrs/ADR-0007-flink-stream-processing.md) | Apache Flink — exactly-once, event-time, RocksDB | Accepted |
| [ADR-0008](docs/adrs/ADR-0008-data-product-ownership.md) | Data Product Ownership Model | Accepted |

---

## Patterns Demonstrated

| Pattern | Where |
|---|---|
| **CDC via Debezium** | WAL-based capture from PostgreSQL; no polling; sub-second latency |
| **Kappa Architecture** | Single Flink streaming path; no batch layer; reprocessing via Kafka replay |
| **Medallion Architecture** | Bronze (raw) → Silver (validated) → Gold (aggregated, serving-ready) |
| **Avro Schema Evolution** | BACKWARD_TRANSITIVE compatibility; field IDs in Iceberg survive renames |
| **Exactly-once semantics** | Flink checkpointing + Kafka COMMITTED isolation + Iceberg transactional commit |
| **Watermarking & late events** | Event-time windows with configurable tolerance; late arrivals to DLQ side output |
| **Dead Letter Queue** | Malformed / duplicate / late events to `chakra.orders.dlq`; full lineage preserved |
| **Data Product ownership** | Domain teams own `contracts/data-products/`; platform team owns the runtime |

---

## Contract-First Development

`contracts/` is the only directory domain teams author directly.

```
contracts/
├── schemas/           Avro schemas — the wire format for every Kafka topic
├── data-products/     SLAs: freshness, completeness, availability, DLQ rate
└── lineage/           Expected data flow; compliance agent checks topology matches
```

The pipeline derives everything from these files:

```
contracts/schemas/*.avsc         →  Debezium serialization config
contracts/schemas/*.avsc         →  Flink deserialization + Silver schema
contracts/data-products/*.yaml   →  Iceberg table definitions (Gold schema)
contracts/data-products/*.yaml   →  observability/slos/*.yaml + alerts
contracts/data-products/*.yaml   →  Kafka retention configuration
```

---

## Technology Stack

| Concern | Choice |
|---|---|
| CDC | Debezium (PostgreSQL pgoutput plugin) |
| Message bus | Apache Kafka (Amazon MSK) + Confluent Schema Registry |
| Stream processing | Apache Flink (PyFlink) |
| Lakehouse storage | Apache Iceberg v2 on S3 |
| Iceberg catalog | AWS Glue Data Catalog |
| Transformation (Gold) | dbt Core |
| Serving (analysts) | DuckDB + PyIceberg |
| Serving (production scale) | AWS Athena (see ADR-0005) |
| Observability | OTEL → Grafana (Tempo / Loki / Mimir) + Flink metrics → Prometheus |
| Infrastructure | Terraform (MSK, EKS, S3, Glue) + Helm (Debezium, Flink) |

---

## Repository Map

```
contracts/         Human-authored source of truth
  schemas/         Avro schemas for all Kafka topics
  data-products/   Data product SLAs (freshness, quality, Gold table schema)
  lineage/         Expected pipeline topology (checked by compliance agent)

docs/
  adrs/            8 Architecture Decision Records
  architecture/    Pipeline flow diagrams, medallion layer overview

pipeline/
  debezium/        Connector configs (generated from schemas/ by agent)
  flink/           PyFlink jobs: Bronze→Silver transformation
  dbt/             dbt models: Silver→Gold aggregations

serving/
  duckdb/          Catalog init, reference queries, local fixtures
  athena/          Production-scale migration guide (ADR-0005)

observability/
  slos/            SLO YAML (derived from data-products/ by script)
  alerts/          PrometheusRule burn rate alerts

ai-agents/
  tasks/agent/     LLM task specs: implement Flink jobs, dbt models
  tasks/script/    Script specs: generate connector configs, Iceberg DDL
  context/         Pipeline conventions, data quality requirements
```
