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
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH && {cmd}",
        ],
        check=True,
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

        
        cutoff_ts = (
            datetime.utcnow() - timedelta(hours=1)
        ).strftime("%Y-%m-%d %H:%M:%S")

        # Ensure HDFS path exists
        hdfs("hdfs dfs -mkdir -p /bda/raw/mongo")

        # Remove previous raw file
        hdfs(f"hdfs dfs -rm -f {HDFS_RAW_PATH}")

        proc = subprocess.Popen(
            [
                "docker", "exec", "-i", NAMENODE,
                "bash", "-lc",
                (
                    "export PATH=/opt/hadoop-3.2.1/bin:$PATH && "
                    f"hdfs dfs -put -f - {HDFS_RAW_PATH}"
                ),
            ],
            stdin=subprocess.PIPE,
            text=True,
        )

        wrote_any = False

        
        cursor = coll.find(
            {"event_timestamp": {"$lt": cutoff_ts}}
        )

        for doc in cursor:
            wrote_any = True
            doc["_id"] = str(doc["_id"])
            proc.stdin.write(json.dumps(doc) + "\n")

        proc.stdin.close()
        rc = proc.wait()

        if rc != 0:
            raise RuntimeError("HDFS put failed")

       
        coll.delete_many({"event_timestamp": {"$lt": cutoff_ts}})

        client.close()

        if not wrote_any:
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
