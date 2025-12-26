from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os

# =========================
# CONFIG (env-driven)
# =========================
SPARK_CONTAINER = os.environ.get("SPARK_CONTAINER", "spark")
SPARK_APP_PATH = os.environ.get(
    "SPARK_APP_PATH",
    "/app/spark_analytics_dashboard_2.py"
)
SPARK_APP_PATH2 = os.environ.get(
    "SPARK_APP_PATH2",
    "/app/spark_analytics_dashboard_2.py"
)

SUPERSET_CONTAINER = os.environ.get("SUPERSET_CONTAINER", "superset")
SUPERSET_REFRESH_CMD = os.environ.get(
    "SUPERSET_REFRESH_CMD",
    "superset dashboard refresh"
)

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="bda_spark_kpi_and_dashboard",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",   # every 5 minutes
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["bda", "spark", "kpi", "superset"],
) as dag:

    # =========================
    # SPARK KPI JOB
    # =========================
    run_spark_kpis = BashOperator(
        task_id="run_spark_kpis",
        bash_command=r"""
        set -e
        /usr/bin/docker exec -i {{ params.spark_container }} bash -lc '
          export PATH=$PATH:/opt/spark/bin
          rm -rf /tmp/spark-local && mkdir -p /tmp/spark-local && chmod 777 /tmp/spark-local

          SPARK_LOCAL_DIRS=/tmp/spark-local spark-submit \
            --master local[1] \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.sql.shuffle.partitions=1 \
            --conf spark.default.parallelism=1 \
            --conf spark.sql.adaptive.enabled=false \
            --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2,org.postgresql:postgresql:42.7.3 \
            {{ params.spark_app }}
        '
        """,
        params={
            "spark_container": SPARK_CONTAINER,
            "spark_app": SPARK_APP_PATH2,
        },
    )

    # =========================
    # DAG ORDER
    # =========================
    run_spark_kpis
