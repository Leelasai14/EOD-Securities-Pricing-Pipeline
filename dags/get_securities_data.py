# dags/get_securities_data.py

# import required libraries and modules
from airflow import DAG  # To define the DAG and orchestrate tasks 
from airflow.exceptions import AirflowFailException  # To raise exceptions in case of failures
from airflow.providers.standard.operators.python import PythonOperator  # To execute Python functions as tasks
from airflow.models import Variable # To fetch variables from Airflow UI or environment
import logging # For logging events
import pendulum  # For handling dates and time in Airflow tasks
import os
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import TaskGroup

# Helpers
from lib.slack_utils import slack_post, on_task_failure
from lib.eod_data_downloader import download_polygon_eod_data_to_csv  # Custom function to download EOD data from Polygon API


# Setup basic configurations for Polygon API
POLYGON_API_KEY = Variable.get("POLYGON_API_KEY")   #API Key for Polygon.io to access market data
POLYGON_MAX_LOOKBACK_DAYS = int(Variable.get("LOOKBACK_DAYS", default_var="10"))  # Maximum number of days to look back for trading data
S3_BUCKET = Variable.get("S3_BUCKET")  # S3 bucket name to upload the data
TEMPLATE_SEARCHPATH = [os.path.join(os.path.dirname(__file__), "sql")]

# Initialize logger for logging events
log = logging.getLogger(__name__)

# Default arguments for the DAG
DEFAULT_ARGS = {"owner": "data-eng",  # Owner of the DAG
                "retries": 3, # Retry the task 3 times if it fails
                "retry_delay": pendulum.duration(minutes=5),  # Retry delay of 5 minutes
                }


# Define the DAG
with DAG(
    dag_id="polygon_eod_data_downloader_final_v2",  # Unique identifier for this DAG
    start_date=pendulum.datetime(2026, 11, 1),  # Start date for the DAG execution
    schedule="5 21 * * 1-5",  # Scheduled to run Mon-Fri at 21:05 UTC
    catchup=False,  # Don't backfill past missed runs
    max_active_runs=1,  # Only allow one active DAG run at a time
    default_args=DEFAULT_ARGS,  # Default arguments for task retries and failure handling
    tags=["securities", "batch", "polygon"],  # Tags for categorization in the Airflow UI,
    on_failure_callback=on_task_failure,
    template_searchpath=TEMPLATE_SEARCHPATH,
    description="Polygon-only batch EOD: Download and process the latest available trading day.",
) as dag:

    # Step: Download the trading day's data to CSV (imported from lib)
    def download_trading_day_csv(**ctx):
        """
        This function downloads the Polygon EOD data 
        and stores it as a CSV file in the specified location.
        """

        # Use the function from lib/eod_data_downloader.py to download the data for the fixed date
        trading_date = download_polygon_eod_data_to_csv(POLYGON_API_KEY, POLYGON_MAX_LOOKBACK_DAYS)

        # Push the trading day to XCom for further tasks if needed
        ctx["ti"].xcom_push(key="trading_date", value=trading_date)

        # Log the success of the task
        log.info(f"Downloaded EOD data for {trading_date}")


    # PythonOperator to call the download function
    download = PythonOperator(
        task_id="t01_download_to_csv",   # Task ID for Airflow UI
        python_callable=download_trading_day_csv,  # Function to execute for this task
    )

    # Step : Verify local file 
    def verify_file_exists(**ctx):
        """
        This function checks if the expected CSV file exists at the given local path.
        If not, it raises an AirflowFailException.
        """

        # Get the trading date from XCom (from the previous task)
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")  # Ensure consistency with key name
        path = f"/tmp/eod_{trading_date}.csv"  # Construct the path of the file
        log.info("[verify] expecting file at: %s", path)

         # Check if the file exists locally
        if not os.path.exists(path):
            raise AirflowFailException(f"Expected file not found: {path}")
        
        # Log the file size if it exists
        log.info("[verify] file exists at %s (size=%s bytes)", path, os.path.getsize(path))


    # PythonOperator to call the Verification function
    verify_file = PythonOperator(
                task_id="t02_verify_local_file", 
                python_callable=verify_file_exists)

    # Step: Upload to S3 
    upload_file = LocalFilesystemToS3Operator(
        task_id="t03_upload_to_s3",
        filename="/tmp/eod_{{ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date')}}.csv",
        dest_bucket=S3_BUCKET, # S3 bucket where the file will be uploaded
        dest_key=(
            "market/bronze/eod/"
            "eod_prices_{{ ti.xcom_pull(task_ids='t01_download_to_csv', key='trading_date') }}.csv"
        ),
        aws_conn_id="aws_default",  # AWS connection ID to fetch credentials
        replace=True,  # Replace the file if it already exists in S3
    )

    # Step : Snowflake load
    with TaskGroup(group_id="t04_snowflake_load") as snowflake_load:
        params_common = {"trading_ds_task_id": "t01_download_to_csv"}
        copy_to_raw = SQLExecuteQueryOperator(
            task_id="s01_copy_to_raw",
            conn_id="snowflake_default",
            sql="1. copy_to_raw.sql",
            params=params_common,
        )

        check_loaded = SQLExecuteQueryOperator(
            task_id="s02_check_loaded_for_dt",
            conn_id="snowflake_default",
            sql="2. check_loaded.sql",
            params=params_common,
        )

        premerge_metrics = SQLExecuteQueryOperator(
            task_id="s03_compute_premerge_metrics",
            conn_id="snowflake_default",
            sql="3. premerge_metrics.sql",
            params=params_common,
        )

        merge_core = SQLExecuteQueryOperator(
            task_id="s04_merge_core_eod",
            conn_id="snowflake_default",
            sql="4. merge_core.sql",
            params=params_common,
        )

        merge_dim_security = SQLExecuteQueryOperator(
            task_id="s05_merge_dim_security",
            conn_id="snowflake_default",
            sql="5. merge_dim_security.sql",
            params=params_common,
        )

        merge_dim_date = SQLExecuteQueryOperator(
            task_id="s06_merge_dim_date",   
            conn_id="snowflake_default",
            sql="6. dm_dim_date.sql",
            params=params_common,
        )

        merge_fact = SQLExecuteQueryOperator(
            task_id="s07_merge_fact_daily_price",
            conn_id="snowflake_default",
            sql="7. merge_fact_daily_price.sql",
            params=params_common,
        )

        postmerge = SQLExecuteQueryOperator(
            task_id="s08_compute_postmerge_metrics",
            conn_id="snowflake_default",
            sql="8. postmerge_metrics.sql",
            params=params_common,
        )

        copy_to_raw >> check_loaded >> premerge_metrics >> merge_core 
        merge_core >> [merge_dim_security, merge_dim_date] >> merge_fact >> postmerge

    # Wiring for extract → verify → upload → Snowflake TG
    download >> verify_file >> upload_file >> snowflake_load

    # Step: Slack summary
    def notify_slack_summary(**ctx):
        """
        Sends a summary message to Slack at the end of the DAG.
        Pulls metrics from pre/post merge tasks and posts a compact summary.
        """
        trading_date = ctx["ti"].xcom_pull(task_ids="t01_download_to_csv", key="trading_date")
        pre = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s03_compute_premerge_metrics") or []
        post = ctx["ti"].xcom_pull(task_ids="t04_snowflake_load.s08_compute_postmerge_metrics") or []

        raw_cnt = ins_est = upd_est = core_ds = fact_ds = 0

        # pre = [(raw_cnt, core_existing_cnt, ins_est, upd_est)]
        if pre and len(pre[0]) >= 4:
            raw_cnt, reject_cnt, ins_est, upd_est = pre[0]

        # post = [(core_rows, fact_rows)]
        if post and len(post[0]) >= 2:
            core_ds, fact_ds = post[0]

        msg = (
                ":white_check_mark: *EOD Summary*\n"
                f"• Trading Date: `{trading_date}`\n"
                f"• RAW rows: `{int(raw_cnt):,}`\n"
                f"• Reject rows: `{int(reject_cnt):,}`\n"
                f"• Estimated CORE inserts: `{int(ins_est):,}`\n"
                f"• Estimated CORE updates: `{int(upd_est):,}`\n"
                f"• CORE rows after merge: `{int(core_ds):,}`\n"
                f"• FACT rows after merge: `{int(fact_ds):,}`"
            )
        slack_post(msg)


    slack_summary = PythonOperator(
        task_id="t05_notify_slack_summary",
        python_callable=notify_slack_summary,
        trigger_rule="all_done",   # ensure Slack fires even if an upstream task failed/skipped
    )


    snowflake_load >> slack_summary

