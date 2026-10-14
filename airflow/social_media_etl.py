"""Social Media ETL pipeline (Airflow 3).

Flow: wait for JSON -> validate/transform -> load to HDFS (3 parallel loads)
      -> Hive external table -> analytics table -> report.

Requirements:
- HDFS/YARN running, Hive installed (embedded Derby metastore).
- An Airflow connection named `fs_default` (type: File (path), extra {"path": "/"}).
- Do not run HiveServer2 / another hive shell while this DAG runs (Derby allows one process).
"""

import os
import shlex
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

PROJECT_DIR = os.environ.get(
    "SOCIAL_MEDIA_PROJECT_DIR", os.path.expanduser("~/social-media-etl-pipeline")
)
HDFS_BASE = os.environ.get("SOCIAL_MEDIA_HDFS_DIR", "/social_media").rstrip("/")
HADOOP_HOME = os.environ.get("HADOOP_HOME", "/usr/local/hadoop")
HIVE_HOME = os.environ.get("HIVE_HOME", "/usr/local/hive")
HDFS_CLI = os.environ.get("HDFS_CLI", f"{HADOOP_HOME}/bin/hdfs")
HIVE_CLI = os.environ.get("HIVE_CLI", f"{HIVE_HOME}/bin/hive")

INCOMING = os.path.join(PROJECT_DIR, "data", "incoming", "social_media_posts.json")
MEDIA_DIR = os.path.join(PROJECT_DIR, "data", "media")
PROCESSED = os.path.join(PROJECT_DIR, "data", "processed", "social_media_posts.tsv")
SCRIPTS_DIR = os.path.join(PROJECT_DIR, "scripts")
SQL_DIR = os.path.join(PROJECT_DIR, "sql")
REPORT = os.path.join(PROJECT_DIR, "reports", "social_media_analytics.txt")

HDFS_RAW = f"{HDFS_BASE}/raw"
HDFS_MEDIA = f"{HDFS_BASE}/media"
HDFS_PROCESSED = f"{HDFS_BASE}/processed"

q = shlex.quote
hdfs = q(HDFS_CLI)
hive = q(HIVE_CLI)

# BashOperator does not load ~/.bashrc, so set the environment explicitly.
ENV = f"""set -e
export HADOOP_HOME={q(HADOOP_HOME)}
export HIVE_HOME={q(HIVE_HOME)}
"""


def validate_and_transform():
    """Validate the input JSON and write the TSV used by Hive."""
    sys.path.insert(0, SCRIPTS_DIR)
    import validate_social_media

    os.makedirs(os.path.dirname(PROCESSED), exist_ok=True)
    count = validate_social_media.run(INCOMING, PROCESSED)
    print(f"Validated {count} posts -> {PROCESSED}")


with DAG(
    dag_id="social_media_etl",
    description="Validate, load, transform and analyze social media posts",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "jyoti", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["social-media", "etl", "hive", "hdfs"],
) as dag:

    wait_for_json = FileSensor(
        task_id="wait_for_json",
        filepath=INCOMING,
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=60 * 60,
        mode="poke",
    )

    validate_and_transform_task = PythonOperator(
        task_id="validate_and_transform",
        python_callable=validate_and_transform,
    )

    load_raw_json_to_hdfs = BashOperator(
        task_id="load_raw_json_to_hdfs",
        bash_command=ENV + f"""
{hdfs} dfs -mkdir -p {q(HDFS_RAW)}
{hdfs} dfs -put -f {q(INCOMING)} {q(HDFS_RAW + "/social_media_posts.json")}
{hdfs} dfs -ls {q(HDFS_RAW)}
""",
    )

    load_raw_media_to_hdfs = BashOperator(
        task_id="load_raw_media_to_hdfs",
        bash_command=ENV + f"""
{hdfs} dfs -mkdir -p {q(HDFS_MEDIA)}
for f in {q(MEDIA_DIR)}/*; do
  [ -f "$f" ] || continue
  [ "$(basename "$f")" = "README.md" ] && continue
  {hdfs} dfs -put -f "$f" {q(HDFS_MEDIA)}/
done
{hdfs} dfs -ls {q(HDFS_MEDIA)}
""",
    )

    load_processed_tsv_to_hdfs = BashOperator(
        task_id="load_processed_tsv_to_hdfs",
        bash_command=ENV + f"""
test -s {q(PROCESSED)}
{hdfs} dfs -mkdir -p {q(HDFS_PROCESSED)}
{hdfs} dfs -put -f {q(PROCESSED)} {q(HDFS_PROCESSED + "/social_media_posts.tsv")}
{hdfs} dfs -ls {q(HDFS_PROCESSED)}
""",
    )

    create_hive_table = BashOperator(
        task_id="create_hive_table",
        bash_command=ENV + f"""
{hive} --hivevar HDFS_PROCESSED={q(HDFS_PROCESSED)} -f {q(SQL_DIR + "/01_create_table.sql")}
""",
    )

    create_analytics_table = BashOperator(
        task_id="create_analytics_table",
        bash_command=ENV + f"""
{hive} -f {q(SQL_DIR + "/02_analytics.sql")}
""",
    )

    generate_report = BashOperator(
        task_id="generate_report",
        bash_command=ENV + f"""
mkdir -p {q(os.path.dirname(REPORT))}
{{
  echo "Social Media Analytics Report"
  echo "Generated at: $(date)"
  echo
  {hive} -S -f {q(SQL_DIR + "/03_report.sql")}
}} > {q(REPORT)}
cat {q(REPORT)}
""",
    )

    wait_for_json >> validate_and_transform_task
    validate_and_transform_task >> [
        load_raw_json_to_hdfs,
        load_raw_media_to_hdfs,
        load_processed_tsv_to_hdfs,
    ] >> create_hive_table
    create_hive_table >> create_analytics_table >> generate_report
