from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
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
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH && {cmd}",
        ],
        check=True,
    )


with DAG(
    dag_id="mongo_raw_data_archival",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1, # concurrency=1,  # optional
    default_args=default_args,
) as dag:

    @task
    def dump_mongo_to_hdfs():
        client = MongoClient(MONGO_URI)
        coll = client[MONGO_DB][MONGO_COLLECTION]

        cutoff_ts = (
            datetime.utcnow() - timedelta(hours=1)
        ).strftime("%Y-%m-%d %H:%M:%S")
        query = {"event_timestamp": {"$lt": cutoff_ts}}

        # Ensure HDFS path exists
        hdfs("hdfs dfs -mkdir -p /bda/raw/mongo")

        ctx = get_current_context()
        run_id = ctx["run_id"].replace(":", "_")
        tmp_path = f"/bda/raw/mongo/raw_{run_id}.json"

        # write to tmp file (no rm of raw.json)
        proc = subprocess.Popen(
            [
                "docker", "exec", "-i", NAMENODE,
                "bash", "-lc",
                (
                    "export PATH=/opt/hadoop-3.2.1/bin:$PATH && "
                    f"hdfs dfs -put -f - {tmp_path}"
                ),
            ],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,   # ✅ capture real error
            text=True,
            bufsize=1,
        )

        wrote_any = False
        try:
            for doc in coll.find(query):
                if proc.poll() is not None:
                    err = (proc.stderr.read() or "").strip()
                    raise RuntimeError(f"HDFS put exited early.\nSTDERR:\n{err}")
                wrote_any = True
                doc["_id"] = str(doc["_id"])
                proc.stdin.write(json.dumps(doc) + "\n")
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass

        rc = proc.wait()
        err = (proc.stderr.read() or "").strip()
        client.close()

        if rc != 0:
            raise RuntimeError(f"HDFS put failed (rc={rc}).\nSTDERR:\n{err}")

        # atomic-ish publish: rename tmp -> raw.json
        hdfs(f"hdfs dfs -mv -f {tmp_path} {HDFS_RAW_PATH}")

        if wrote_any:
            coll.delete_many(query)
        else:
            print("No records older than cutoff — nothing archived this run.")

    spark_archive = BashOperator(
        task_id="spark_archive",
        bash_command=r"""
        set -e
        /usr/bin/docker exec -i ${SPARK_CONTAINER:-spark} bash -lc '
          export PATH=$PATH:/opt/spark/bin
          export HADOOP_USER_NAME=root

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
