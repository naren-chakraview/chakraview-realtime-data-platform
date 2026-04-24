# ADR-0008: Data Product Ownership Model

**Status**: Accepted
**Date**: 2024-04-01
**Deciders**: Data platform team, domain teams

---

## Context

In a centralized data platform model, the data engineering team owns all pipelines, schemas, and SLAs. Domain teams file tickets when they need new data or schema changes. This creates a bottleneck and separates the people who understand the data (domain teams) from the people who publish it.

The alternative — a Data Mesh approach — treats data as a product: each domain team owns the data it produces, including the schema, SLA, and quality guarantees.

---

## Decision

Adopt a **data product ownership model**. Each bounded context (Orders, Inventory, Customers) owns its data product: the Gold-layer Iceberg tables that represent its domain, the Avro schemas for its events, and the freshness/quality SLAs.

The data platform team owns the **platform** — the Kafka cluster, Debezium connectors, Flink runtime, Iceberg catalog, and DuckDB/Athena query layer. Domain teams own the **contracts** that flow through the platform.

### What a data product owns

A data product is defined by a `contracts/data-products/{name}.yaml` file that specifies:
- **Schema**: Which Avro schema versions are valid inputs
- **SLAs**: Freshness, completeness, availability, and quality targets
- **Kafka retention**: How long the source topic must retain messages (drives Kappa reprocessing window)
- **Gold-layer schema**: The public interface consumers depend on
- **Owner team**: Who is paged when the data product SLA is breached

### What the platform owns

- Debezium connector lifecycle (deploy, monitor, upgrade)
- Flink job runtime (Job Manager, Task Managers, checkpointing)
- Iceberg catalog and compaction schedule
- Schema Registry configuration and compatibility rules
- Observability (SLO definitions derived from `contracts/data-products/`)

---

## Consequences

**Positive:**
- Domain teams can evolve their schemas and SLAs without filing platform tickets.
- SLA breaches page the domain team that owns the data, not the platform team.
- The `contracts/data-products/` directory is auditable: every change to a data product SLA goes through PR review by the owning team (`CODEOWNERS`).

**Negative:**
- Domain teams must understand enough about Avro and Iceberg to author valid contracts. This requires investment in documentation and onboarding.
- Conflicting SLAs across data products (e.g., Orders wants 1-minute freshness, platform can deliver 2-minute) must be resolved at contract authoring time, not at incident time.

---

## Related

- `contracts/data-products/` — All data product definitions
- `CODEOWNERS` — Domain team ownership of their data product contract files
- [ADR-0006](ADR-0006-avro-schema-registry.md) — Schema governance that supports domain ownership
