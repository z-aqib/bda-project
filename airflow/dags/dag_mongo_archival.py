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
# Helper functions (UNCHANGED – as requested)
# ============================================================
def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)

def _hdfs(container: str, cmd: str) -> str:
    p = subprocess.run(
        [
            "/usr/bin/docker",
            "exec",
            "-i",
            container,
            "bash",
            "-lc",
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH; {cmd}",
        ],
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"HDFS command failed\n{p.stderr}")
    return (p.stdout or "").strip()

def _coll_size_bytes(mongo_uri: str, db: str, coll: str) -> int:
    client = MongoClient(mongo_uri)
    size = client[db].command("collStats", coll).get("size", 0)
    client.close()
    return int(size)

def _max_event_ts(client: MongoClient, db: str, coll: str):
    doc = (
        client[db][coll]
        .find({}, {"event_timestamp": 1})
        .sort("event_timestamp", -1)
        .limit(1)
    )
    lst = list(doc)
    return lst[0]["event_timestamp"] if lst else None

def _free_bytes(path: str) -> int:
    return int(shutil.disk_usage(path).free)

def _pipe_local_file_into_container(container: str, local_path: str, container_tmp_path: str):
    subprocess.run(
        [
            "bash",
            "-lc",
            f"cat '{local_path}' | docker exec -i {container} bash -lc 'cat > {container_tmp_path}'",
        ],
        check=True,
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
    tags=["mongo", "archive", "raw"],
):

    @task
    def archive_raw_sensor_data():

        # ----------------------------
        # Config
        # ----------------------------
        MONGO_URI = "mongodb://mongo:27017"
        MONGO_DB = "iot_database"
        MONGO_COLL = "sensor_readings"

        KEEP_MINUTES = 60
        LOCAL_TMP = "/opt/airflow/archive_tmp"

        SPARK_CONTAINER = "spark"
        HDFS_CONTAINER = "namenode"

        HDFS_DATA_BASE = "/bda/archive/data"
        HDFS_META_BASE = "/bda/archive/metadata"

        os.makedirs(LOCAL_TMP, exist_ok=True)

        # ----------------------------
        # Disk safety
        # ----------------------------
        if _free_bytes(LOCAL_TMP) < 800 * 1024 * 1024:
            return {"status": "skipped", "reason": "low_disk_space"}

        # ----------------------------
        # Mongo cutoff
        # ----------------------------
        client = MongoClient(MONGO_URI)
        coll = client[MONGO_DB][MONGO_COLL]

        max_ts = _max_event_ts(client, MONGO_DB, MONGO_COLL)
        if not max_ts:
            return {"status": "skipped", "reason": "no_data"}

        cutoff_dt = (
            datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
            - timedelta(minutes=KEEP_MINUTES)
        )
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

        query = {"event_timestamp": {"$lt": cutoff_str}}
        docs = list(coll.find(query))

        if not docs:
            return {"status": "skipped", "reason": "no_eligible_records"}

        # ----------------------------
        # Write raw JSON locally
        # ----------------------------
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
        local_json = f"{LOCAL_TMP}/raw_{run_id}.json"

        with open(local_json, "w") as f:
            for d in docs:
                d["_id"] = str(d["_id"])
                f.write(json.dumps(d) + "\n")

        # ----------------------------
        # Ensure HDFS dirs
        # ----------------------------
        _hdfs(HDFS_CONTAINER, f"hdfs dfs -mkdir -p {HDFS_DATA_BASE}")
        _hdfs(HDFS_CONTAINER, f"hdfs dfs -mkdir -p {HDFS_META_BASE}")

        # ----------------------------
        # Run Spark INSIDE spark container
        # ----------------------------
        spark_cmd = f"""
        docker exec -i {SPARK_CONTAINER} bash -lc '
        /opt/spark/bin/spark-submit \
          --conf spark.sql.session.timeZone=UTC \
          << "EOF"
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour

spark = SparkSession.builder.appName("mongo-archive").getOrCreate()

df = spark.read.json("{local_json}")
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
 .parquet("{HDFS_DATA_BASE}")
)

spark.stop()
EOF
'
        """

        subprocess.run(["bash", "-lc", spark_cmd], check=True)

        # ----------------------------
        # Delete archived docs
        # ----------------------------
        delete_res = coll.delete_many(query)

        # ----------------------------
        # Metadata
        # ----------------------------
        metadata = {
            "run_id": run_id,
            "archived_at_utc": datetime.now(timezone.utc).isoformat(),
            "cutoff_timestamp": cutoff_str,
            "documents_archived": len(docs),
            "documents_deleted": delete_res.deleted_count,
            "hdfs_data_path": HDFS_DATA_BASE,
            "policy": "30 min archive, keep 1 hour hot, parquet per factory"
        }

        meta_file = f"{LOCAL_TMP}/meta_{run_id}.json"
        with open(meta_file, "w") as mf:
            json.dump(metadata, mf, indent=2)

        _pipe_local_file_into_container(
            HDFS_CONTAINER,
            meta_file,
            f"/tmp/meta_{run_id}.json"
        )

        _hdfs(
            HDFS_CONTAINER,
            f"hdfs dfs -put -f /tmp/meta_{run_id}.json {HDFS_META_BASE}/meta_{run_id}.json"
        )

        client.close()
        return metadata

    archive_raw_sensor_data()
