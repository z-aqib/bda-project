from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import json
import subprocess
import os

# Pull from env (matches airflow-docker-compose.yml)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/iot_database")
MONGO_DB = os.environ.get("MONGO_DB", "iot_database")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

NAMENODE = os.environ.get("HDFS_CONTAINER", "namenode")

# Use plain HDFS path for CLI, Spark can still read via hdfs:// URI
HDFS_RAW_PATH = "/bda/raw/mongo/raw.json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def hdfs(cmd: str):
    subprocess.run(
        [
            "docker", "exec", NAMENODE,
            "bash", "-lc",
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH && {cmd}"
        ],
        check=True
    )

with DAG(
    dag_id="mongo_raw_data_archival",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    @task
    def dump_mongo_to_hdfs():
        client = MongoClient(MONGO_URI)
        coll = client[MONGO_DB][MONGO_COLLECTION]

        # Ensure directory exists
        hdfs("hdfs dfs -mkdir -p /bda/raw/mongo")

        # Remove old file if present
        hdfs(f"hdfs dfs -rm -f {HDFS_RAW_PATH}")

        # IMPORTANT: export PATH here too, and FAIL the task if put fails
        proc = subprocess.Popen(
            [
                "docker", "exec", "-i", NAMENODE,
                "bash", "-lc",
                f"export PATH=/opt/hadoop-3.2.1/bin:$PATH && hdfs dfs -put -f - {HDFS_RAW_PATH}"
            ],
            stdin=subprocess.PIPE,
            text=True,
        )

        wrote_any = False
        for doc in coll.find():
            wrote_any = True
            doc["_id"] = str(doc["_id"])
            proc.stdin.write(json.dumps(doc) + "\n")

        proc.stdin.close()
        rc = proc.wait()
        client.close()

        if rc != 0:
            raise RuntimeError(f"HDFS put failed (return code={rc}). File not written: {HDFS_RAW_PATH}")

        # Optional: if no docs, still create an empty file (Spark may later need schema handling)
        if not wrote_any:
            print("WARNING: Mongo collection had 0 docs; raw.json will be empty.")

    spark_archive = BashOperator(
        task_id="spark_archive",
        bash_command=r"""
        set -e
        /usr/bin/docker exec -i ${SPARK_CONTAINER:-spark} bash -lc '
          export PATH=$PATH:/opt/spark/bin

          rm -rf /tmp/spark-local
          mkdir -p /tmp/spark-local
          chmod 777 /tmp/spark-local

          SPARK_LOCAL_DIRS=/tmp/spark-local spark-submit \
            --master local[1] \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.sql.shuffle.partitions=1 \
            --conf spark.default.parallelism=1 \
            --conf spark.sql.adaptive.enabled=false \
            ${SPARK_APP_PATH:-/app/mongo_raw_to_parquet.py}
        '
        """,
    )

    dump_mongo_to_hdfs() >> spark_archive
