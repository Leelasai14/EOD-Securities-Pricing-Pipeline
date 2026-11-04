# EOD Securities Pricing Pipeline

An automated daily pipeline that downloads U.S. stock end-of-day (EOD) prices from the Massive API (formerly Polygon.io), loads them into a layered Snowflake warehouse, and serves them to Power BI. Airflow runs it every weekday and posts the result to Slack.

**Stack:** Massive API (formerly Polygon.io) · Python · Apache Airflow (Docker) · AWS S3 · Snowflake · SQL · Slack · Power BI

---

## 30-Second Summary (for interviews)

> "I built a daily batch pipeline. Airflow downloads end-of-day prices from the Massive API (formerly Polygon.io), uploads the CSV to S3, and Snowflake loads it with COPY INTO into a RAW table. SQL MERGE statements then clean and upsert the data through CORE, dimension and fact tables, so reruns never create duplicates. Slack gets a failure alert or a run summary, and Power BI reads curated views on top."

---

## Note: Polygon.io is now Massive

Polygon.io rebranded as **Massive** (massive.com) on Oct 30, 2025. Existing API keys keep working. The new API base is `https://api.massive.com`, and `https://api.polygon.io` is still supported for an extended period. This project uses the Massive API; the DAG id and the `POLYGON_API_KEY` variable keep the old name only for backward compatibility.

---

## Problem

Analysts needed fresh market data every morning. Downloading CSV files and rebuilding reports by hand is slow and easy to get wrong. This project automates it: data lands, gets cleaned, and is ready for dashboards without manual work.

---

## Architecture

<img width="1024" height="572" alt="image" src="https://github.com/user-attachments/assets/3efef584-ed07-4c4a-b037-f48e6f0eabdf" />


| Layer | What it holds |
|---|---|
| **S3 Bronze** | Raw CSV files, one per trading day |
| **RAW** | Data exactly as loaded, plus source file name and load time |
| **CORE** | Cleaned data, one row per symbol per day |
| **DIM / FACT** | Star schema: `DIM_SECURITY`, `DIM_DATE`, `FACT_DAILY_PRICE` |
| **SA (Subject Area)** | Business-ready views for Power BI |

---

## Airflow DAG

**DAG id:** `polygon_eod_data_downloader_final_v2`
**Schedule:** `5 21 * * 1-5` (Mon-Fri, 21:05 UTC) · **Retries:** 3 (5 min apart) · **Max active runs:** 1 · **Catchup:** off

```
t01_download_to_csv → t02_verify_local_file → t03_upload_to_s3 → t04_snowflake_load → t05_notify_slack_summary
```

| Task | What it does |
|---|---|
| `t01_download_to_csv` | Calls the Massive grouped daily endpoint, finds the latest trading day (looks back over weekends/holidays), saves a CSV, pushes `trading_date` to XCom |
| `t02_verify_local_file` | Fails early if the CSV is missing |
| `t03_upload_to_s3` | Uploads to `market/bronze/eod/YYYY/MM/DD/eod_prices_<date>.csv` |
| `t04_snowflake_load` | TaskGroup with 8 SQL steps (below) |
| `t05_notify_slack_summary` | Posts the run summary to Slack |

### Snowflake TaskGroup (`t04_snowflake_load`)

| Step | SQL file | Purpose |
|---|---|---|
| `s01` | `1. copy_to_raw.sql` | `COPY INTO` RAW from the S3 external stage |
| `s02` | `2. check_loaded.sql` | Fail if no rows exist for the trading date |
| `s03` | `3. premerge_metrics.sql` | Estimate inserts vs updates before merging |
| `s04` | `4. merge_core.sql` | Dedupe and upsert into CORE |
| `s05` | `5. merge_dim_security.sql` | Add new symbols to `DIM_SECURITY` |
| `s06` | `6. dm_dim_date.sql` | Add new dates to `DIM_DATE` |
| `s07` | `7. merge_fact_daily_price.sql` | Upsert `FACT_DAILY_PRICE` |
| `s08` | `8. postmerge_metrics.sql` | Row counts after the merge |

```
s01 → s02 → s03 → s04 → [s05, s06] → s07 → s08
```

`s05` and `s06` run in parallel because they do not depend on each other.

---

## Key Concepts Used (quick revision)

| Concept | How it is used here |
|---|---|
| **XCom** | `t01` pushes `trading_date`; later tasks and SQL files read it |
| **Jinja templates** | SQL and S3 paths use `{{ ti.xcom_pull(...) }}` so the same code runs for any date |
| **`params`** | `params_common` passes the upstream task id into every SQL file (static config, unlike XCom) |
| **TaskGroup** | Groups the 8 Snowflake steps into one box in the UI |
| **`template_searchpath`** | Lets `sql="1. copy_to_raw.sql"` find files in the `sql/` folder |
| **MERGE (upsert)** | Updates existing rows, inserts new ones |
| **External stage + COPY INTO** | Snowflake reads files directly from S3 |
| **Idempotency** | Rerunning a day gives the same final result (see below) |

---

## Data Quality and Reliability

- **Idempotent loads:** MERGE on `(SYMBOL, TRADE_DATE)` in CORE and `(SECURITY_ID, DATE_SK)` in FACT, so a rerun updates rows instead of duplicating them.
- **Dedup rule:** `ROW_NUMBER()` over symbol and date, keeping the latest ingested record.
- **Clean keys:** symbols are normalized with `UPPER(TRIM(SYMBOL))` before matching.
- **Fail early:** local file check (`t02`) and row-count check (`s02`) stop the run before bad or empty data moves on.
- **Pre/post metrics:** RAW rows, estimated inserts/updates, and final CORE/FACT counts for every run.
- **Retries:** 3 retries with a 5-minute delay for temporary failures.
- **No secrets in code:** API key and bucket are Airflow Variables; AWS, Snowflake and Slack use Airflow Connections.

---

## Slack Monitoring

- **Failure alert:** DAG and task id plus a log link, sent through `on_failure_callback`.
- **Daily summary:** trading date, RAW rows, estimated inserts/updates, CORE rows, FACT rows.

Setup: create a Slack app with an Incoming Webhook, then add an Airflow connection `slack_default` (type: Slack Incoming Webhook). Host is `hooks.slack.com/services`, password is the token part of the URL, schema is `https`. The webhook URL is a secret, so never commit it.

---

## Power BI Dashboards

Power BI reads the SA views directly:

- Market liquidity overview (traded value, sector contribution, ETF liquidity)
- Equity performance and watchlist (top equities by volume, daily returns, OHLC)

<img width="1296" height="727" alt="image" src="https://github.com/user-attachments/assets/26dc437f-5cd2-4b41-9870-4cf317a206bc" />

<img width="2075" height="1200" alt="image" src="https://github.com/user-attachments/assets/e621a831-24f1-4316-8f60-8d6779480f25" />

---

## Project Structure

```
├── dags/
│   ├── get_securities_data.py        # the main DAG
│   ├── lib/
│   │   ├── eod_data_downloader.py    # Massive API download
│   │   └── slack_utils.py            # Slack helpers
│   └── sql/                          # the 8 numbered SQL files
├── snowflake/                        # one-time setup: warehouse, database, schemas, stage
├── historical_load/                  # one-time backfill scripts
├── powerbi/                          # dashboard files
├── resources/                        # diagrams and screenshots
└── README.md
```


---

## How to Run

1. Install Docker Desktop and start Airflow with `docker compose up -d`.
2. In Snowflake, run the setup scripts (warehouse `WH_INGEST`, database `SEC_PRICING`, schemas, tables, external stage `RAW.EXT_BRONZE`).
3. In the Airflow UI, set **Variables:** `POLYGON_API_KEY` (holds your Massive API key), `S3_BUCKET`, `LOOKBACK_DAYS` (optional).
4. Add **Connections:** `aws_default`, `snowflake_default`, `slack_default`.
5. Enable and trigger `polygon_eod_data_downloader_final_v2`.
6. Check Slack for the summary and query `SEC_PRICING.RAW.RAW_EOD_PRICES` to confirm rows loaded.

---

## Lessons Learned (good interview stories)

**1. A green DAG is not proof that data loaded.**
My DAG finished successfully, but Snowflake tables were empty. The S3 upload used date folders (`eod/2026/11/03/...`) while `COPY INTO` looked in `eod/` directly. `COPY INTO` matching zero files is not an error, and every merge ran fine on empty input.
*Fix:* build the S3 key and the COPY path from the same date logic, and make `s02` a real check that fails when zero rows load (`SnowflakeCheckOperator`).

**2. `FORCE = TRUE` breaks idempotency.**
It reloads a file even if Snowflake already loaded it, which creates duplicate RAW rows on every rerun. Better: remove it, or delete that day's RAW rows before loading. CORE still dedupes with MERGE, which is the safety net.

**3. `trigger_rule="all_done"` can hide failures.**
It makes the Slack task run even after a failure, which is useful for alerts, but if that last task succeeds the whole run looks green. Use it carefully, and keep the failure callback.

**4. Slack connection error.**
The host must be `hooks.slack.com/services` without `https://`. The scheme goes in the Schema field.

---

## Interview Q&A

**Why layers (RAW, CORE, DIM/FACT, SA)?**
RAW keeps data as received for replay and audit. CORE is clean and deduplicated. DIM/FACT gives a star schema for fast analytics. SA views hide the complexity from BI users.

**How do you make reruns safe?**
MERGE on business keys, deterministic S3 file names with `replace=True`, and dimension merges that only insert unmatched rows.

**Why XCom plus `params`?**
`params` holds static config known when the DAG is written (which task to ask). XCom holds values created during the run (the trading date).

**How do you handle weekends and holidays?**
The download step looks back day by day within a lookback window until the API returns data.

**What happens if a task fails?**
Retries 3 times, then the failure callback posts to Slack with a log link.

**How would you scale it?**
Partition or cluster large tables, load multiple days in parallel, use Snowflake Tasks or dbt for transformations, and add reconciliation checks against API totals.

---

## Future Improvements

- Reject table for invalid rows (for example, negative volume) with a reason code
- Reconciliation check between API totals and Snowflake counts
- Automated tests for SQL and DAG imports
- CI/CD with GitHub Actions
- Replace manual ID math in `DIM_SECURITY` with a Snowflake sequence

---

