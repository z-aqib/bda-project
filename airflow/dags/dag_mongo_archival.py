from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone
import os
import json
import uuid
import subprocess
import shutil
from pymongo import MongoClient

# ============================================================
# Helpers (exact style you requested)
# ============================================================
def _env(name, default):
    return os.environ.get(name, default)

def _hdfs(container, cmd):
    p = subprocess.run(
        [
            "docker", "exec", "-i", container,
            "bash", "-lc",
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH; {cmd}",
        ],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr)
    return p.stdout

def _pipe_local_file_into_container(container, src, dst):
    subprocess.run(
        ["bash", "-lc", f"cat '{src}' | docker exec -i {container} bash -lc 'cat > {dst}'"],
        check=True
    )

# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="mongo_raw_data_archival",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["archive", "mongo"],
):

    @task
    def archive_raw_sensor_data():

        # ---------------- CONFIG ----------------
        MONGO_URI = _env("MONGO_URI", "mongodb://mongo:27017")
        DB = "iot_database"
        COLL = "sensor_readings"

        KEEP_MINUTES = 60
        LOCAL_TMP = "/opt/airflow/archive_tmp"
        HDFS_DATA = "/bda/archive/data"
        HDFS_META = "/bda/archive/metadata"

        SPARK_CONTAINER = "spark"
        HDFS_CONTAINER = "namenode"

        os.makedirs(LOCAL_TMP, exist_ok=True)

        # ---------------- MONGO ----------------
        client = MongoClient(MONGO_URI)
        coll = client[DB][COLL]

        latest = coll.find_one(sort=[("event_timestamp", -1)])
        if not latest:
            return {"status": "no_data"}

        cutoff = (
            datetime.strptime(latest["event_timestamp"], "%Y-%m-%d %H:%M:%S")
            - timedelta(minutes=KEEP_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")

        query = {"event_timestamp": {"$lt": cutoff}}
        docs = list(coll.find(query))
        if not docs:
            return {"status": "nothing_to_archive"}

        # ---------------- LOCAL JSON ----------------
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
        local_json = f"{LOCAL_TMP}/raw_{run_id}.json"

        with open(local_json, "w") as f:
            for d in docs:
                d["_id"] = str(d["_id"])
                f.write(json.dumps(d) + "\n")

        # ---------------- COPY → SPARK ----------------
        spark_json = f"/tmp/raw_{run_id}.json"
        subprocess.run(
            ["docker", "cp", local_json, f"{SPARK_CONTAINER}:{spark_json}"],
            check=True
        )

        # ---------------- SPARK JOB ----------------
        spark_cmd = f"""
        docker exec -i {SPARK_CONTAINER} bash -lc '
        /opt/spark/bin/spark-submit \
          --conf spark.sql.session.timeZone=UTC \
          << "EOF"
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour

spark = SparkSession.builder.appName("mongo-archive").getOrCreate()

df = spark.read.json("{spark_json}")
df = df.withColumn("event_ts", to_timestamp("event_timestamp"))

(df
 .withColumn("year", year("event_ts"))
 .withColumn("month", month("event_ts"))
 .withColumn("day", dayofmonth("event_ts"))
 .withColumn("hour", hour("event_ts"))
 .repartition("factory_id")
 .write
 .mode("append")
 .partitionBy("factory_id", "year", "month", "day", "hour")
 .parquet("{HDFS_DATA}")
)

spark.stop()
EOF
'
        """
        subprocess.run(["bash", "-lc", spark_cmd], check=True)

        # ---------------- DELETE MONGO ----------------
        delete_res = coll.delete_many(query)

        # ---------------- METADATA ----------------
        meta = {
            "run_id": run_id,
            "archived_at_utc": datetime.now(timezone.utc).isoformat(),
            "cutoff": cutoff,
            "docs_archived": len(docs),
            "docs_deleted": delete_res.deleted_count,
            "hdfs_data_path": HDFS_DATA
        }

        meta_file = f"{LOCAL_TMP}/meta_{run_id}.json"
        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2)

        _hdfs(HDFS_CONTAINER, f"hdfs dfs -mkdir -p {HDFS_META}")
        _pipe_local_file_into_container(
            HDFS_CONTAINER,
            meta_file,
            f"/tmp/meta_{run_id}.json"
        )
        _hdfs(
            HDFS_CONTAINER,
            f"hdfs dfs -put -f /tmp/meta_{run_id}.json {HDFS_META}/meta_{run_id}.json"
        )

        # ---------------- CLEANUP ----------------
        os.remove(local_json)
        os.remove(meta_file)
        subprocess.run(
            ["docker", "exec", SPARK_CONTAINER, "rm", "-f", spark_json],
            check=False
        )

        return meta

    archive_raw_sensor_data()
