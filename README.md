# ShopVista — E-Commerce Lakehouse Data Pipeline

A production-style **Bronze → Silver → Gold** lakehouse pipeline on **Azure Databricks**, built to modernize analytics for an e-commerce platform. It consolidates scattered order, shipment, return, and customer data into a single, governed, analytics-ready warehouse powering a Power BI dashboard.

---

## Problem

Order, shipment, return, and dimension data (customers, products, categories, brands, dates) lived in disconnected CSV exports from a MySQL source system. That meant manual reconciliation before every report, delayed visibility into sales and returns, and no single source of truth for the business.

This project builds the platform that fixes that: automated incremental ingestion, enforced data quality, idempotent processing, and a star-schema warehouse ready for BI consumption.

---

## Architecture

```
MySQL (source) → CSV export → ADLS Gen2 (landing zone)
        → Bronze (raw, append-only)
        → Silver (cleaned, deduplicated, conformed)
        → Gold (business logic, star schema, aggregates)
        → Power BI (dashboards)
```

Governed end-to-end through **Unity Catalog**, orchestrated and scheduled through **Databricks Workflows**.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Source | MySQL | Transactional system of record |
| Storage | Azure Data Lake Storage Gen2 (ADLS) | Centralized raw landing zone |
| Compute / ETL | Azure Databricks (PySpark, Structured Streaming) | Ingestion, transformation, aggregation |
| Governance | Unity Catalog + Access Connector | Catalog/schema permissions, lineage, external volumes |
| Table format | Delta Lake | ACID transactions, MERGE/upsert, Change Data Feed |
| Ingestion | Auto Loader (`cloudFiles`) | Incremental, schema-evolving file ingestion |
| Orchestration | Databricks Workflows | Scheduled, dependency-chained job runs |
| BI | Power BI | Sales, revenue, and returns dashboards |

---

## Data Flow

**Landing** — Raw CSVs (order items, shipments, returns, plus dimension files for customers, products, categories, brands, dates) arrive in an ADLS container, organized by entity.

**Bronze** — Auto Loader streams each file into an append-only Delta table per entity, tagging every row with its source file path and ingestion timestamp. Nothing is cleaned here — Bronze is a faithful, auditable copy of what arrived.

**Silver** — Each Bronze table is deduplicated, type-cast, and standardized: string-encoded numerics have units stripped (`"305g"` → `305`), inconsistent decimal separators are fixed, categorical spelling anomalies are corrected against a lookup table, negative values are normalized, and nulls are handled explicitly (dropped for identifiers, filled for optional fields). Written via `foreachBatch` + Delta `MERGE`, keyed on natural composite keys (e.g. `order_id + item_seq`).

**Gold** — Silver tables are joined and enriched into star-schema fact and dimension tables: products joined to brands/categories, customers mapped to region via a country → state → region lookup, and the order-items fact table enriched with calculated fields (gross amount, discount amount, net amount, coupon flag). Reads Silver via **Change Data Feed** so only inserts/updates are reprocessed, not the full table. A rolling **30-day daily summary** table sits on top, merged by `date_id + currency`, for fast dashboard queries.

**Serving** — Power BI connects directly to Gold for sales, revenue trend, and returns reporting.

---

## Key Engineering Decisions

- **Auto Loader + `trigger(availableNow=True)`** instead of a fully continuous stream — this is a daily-batch workload, not low-latency, so incremental micro-batch processing gives streaming-grade reliability (checkpointing, exactly-once file processing) without paying for an always-on cluster.
- **Change Data Feed on Silver → Gold** — Gold only reprocesses actual row-level changes from Silver instead of rescanning the whole table every run.
- **MERGE (upsert) into Silver and Gold**, keyed on natural composite keys — makes every rerun idempotent instead of producing duplicates, and correctly handles corrections/late-arriving data.
- **Medallion architecture with Unity Catalog schemas per layer** (`bronze` / `silver` / `gold`) instead of separate catalogs — keeps access policies and lineage simple to reason about.
- **Rolling 30-day summary with merge-on-date** — recomputing full history on every run doesn't scale, so the daily aggregate job only touches the last 30 days and merges by `date_id + currency`.
- **Orchestration via Databricks Workflows** — dimension and fact pipelines run as separate jobs with sequential task dependencies, wrapped in a master job on a nightly cron trigger, with email alerts on success/failure.

---

## What I Actually Built

- Full Bronze → Silver → Gold pipeline for all fact and dimension entities (orders, shipments, returns, customers, products, categories, brands, dates), using Auto Loader, Structured Streaming with `foreachBatch` upserts, and Delta `MERGE`.
- Data quality logic: null handling, deduplication, unit/format normalization, categorical standardization, negative-value correction.
- Star-schema Gold tables (products joined to brand/category, customers joined to region) and a rolling daily summary aggregate table.
- Unity Catalog setup: catalog, schemas, and an external volume backed by ADLS, with the Databricks Access Connector for secure storage access.
- End-to-end **orchestration**: multi-task Databricks Jobs with dependency chains, nested "run job" tasks, nightly cron scheduling, and failure/success email notifications.
- Power BI dashboard surfacing total sales, repeat customer rate, sales by brand/category, customer distribution by region, channel split, and monthly revenue trend.

---

## Key Skills Demonstrated

Data pipeline design · Medallion architecture · ETL/ELT · Lakehouse concepts · Delta Lake (MERGE, CDF, streaming) · Cloud data governance (Unity Catalog) · Orchestration & scheduling · Data quality engineering · Star-schema modeling · BI reporting integration
