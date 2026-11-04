# EOD-Securities-Pricing-Pipeline
Daily Airflow pipeline: fetches EOD stock prices from the Massive API (Polygon.io), uploads CSVs to S3, and bulk loads Snowflake RAW tables via COPY INTO. SQL MERGE statements safely upsert data into idempotent CORE dimension and fact tables. Slack provides failure alerts or run summaries, and Power BI reads curated views.
