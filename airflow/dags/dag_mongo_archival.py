from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import json
import subprocess

MONGO_URI = "mongodb://mongo:27017"
DB = "factory"
COLL = "raw_sensor_data"

HDFS_RAW = "/bda/raw/mongo/raw.json"
NAMENODE = "namenode"

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

    # -------------------------
    # Mongo → HDFS
    # -------------------------
    @task
    def dump_mongo_to_hdfs():
        client = MongoClient(MONGO_URI)
        coll = client[DB][COLL]

        hdfs(f"hdfs dfs -rm -f {HDFS_RAW}")

        proc = subprocess.Popen(
            [
                "docker", "exec", "-i", NAMENODE,
                "bash", "-lc",
                f"hdfs dfs -put -f - {HDFS_RAW}"
            ],
            stdin=subprocess.PIPE,
            text=True,
        )

        for doc in coll.find():
            doc["_id"] = str(doc["_id"])
            proc.stdin.write(json.dumps(doc) + "\n")

        proc.stdin.close()
        proc.wait()
        client.close()

    # -------------------------
    # Spark → Parquet (FIXED)
    # -------------------------
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
