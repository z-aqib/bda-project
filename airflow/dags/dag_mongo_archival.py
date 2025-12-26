from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta, timezone
import os, json, uuid, subprocess
from pymongo import MongoClient

# ============================================================
# Helpers (reused exactly as requested)
# ============================================================

def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)

def _hdfs(container: str, cmd: str) -> str:
    p = subprocess.run(
        [
            "/usr/bin/docker", "exec", "-i", container,
            "bash", "-lc",
            f"export PATH=/opt/hadoop-3.2.1/bin:$PATH; {cmd}",
        ],
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"HDFS command failed:\n{p.stderr}")
    return (p.stdout or "").strip()

def _max_event_ts(client, db, coll):
    doc = client[db][coll].find({}, {"event_timestamp": 1}) \
                          .sort("event_timestamp", -1) \
                          .limit(1)
    lst = list(doc)
    return lst[0]["event_timestamp"] if lst else None


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="mongo_raw_data_archival",
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["archive", "mongo", "hdfs"],
):

    @task
    def archive_raw_sensor_data():

        # ---------------- CONFIG ----------------
        MONGO_URI = _env("MONGO_URI", "mongodb://mongo:27017")
        DB = "iot_database"
        COLL = "sensor_readings"
        KEEP_MINUTES = 60

        NAMENODE = "namenode"
        HDFS_RAW_BASE = "/bda/archive/raw"
        HDFS_PARQUET_BASE = "/bda/archive/data"
        HDFS_META_BASE = "/bda/archive/metadata"

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

        raw_hdfs_path = f"{HDFS_RAW_BASE}/raw_{run_id}.jsonl"
        meta_hdfs_path = f"{HDFS_META_BASE}/meta_{run_id}.json"

        # ---------------- Mongo cutoff ----------------
        client = MongoClient(MONGO_URI)
        coll = client[DB][COLL]

        max_ts = _max_event_ts(client, DB, COLL)
        if not max_ts:
            return {"status": "skipped", "reason": "no_data"}

        cutoff_dt = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=KEEP_MINUTES)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

        query = {"event_timestamp": {"$lt": cutoff_str}}
        cursor = coll.find(query)

        # ---------------- HDFS prep ----------------
        _hdfs(NAMENODE, f"hdfs dfs -mkdir -p {HDFS_RAW_BASE}")
        _hdfs(NAMENODE, f"hdfs dfs -mkdir -p {HDFS_PARQUET_BASE}")
        _hdfs(NAMENODE, f"hdfs dfs -mkdir -p {HDFS_META_BASE}")

        # ---------------- Write RAW JSONL directly to HDFS ----------------
        write_cmd = f"hdfs dfs -put -f - {raw_hdfs_path}"
        proc = subprocess.Popen(
            ["/usr/bin/docker", "exec", "-i", NAMENODE, "bash", "-lc", write_cmd],
            stdin=subprocess.PIPE,
            text=True,
        )

        count = 0
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            proc.stdin.write(json.dumps(doc) + "\n")
            count += 1

        proc.stdin.close()
        proc.wait()

        if count == 0:
            return {"status": "skipped", "reason": "nothing_to_archive"}

        # ---------------- Spark: HDFS → Parquet ----------------
        spark_cmd = f"""
        docker exec -i spark bash -lc '
        /opt/spark/bin/spark-submit \
          --conf spark.sql.session.timeZone=UTC \
          << "EOF"
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour

spark = SparkSession.builder.appName("mongo-archive").getOrCreate()

df = spark.read.json("hdfs://namenode:8020{raw_hdfs_path}")
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
 .parquet("hdfs://namenode:8020{HDFS_PARQUET_BASE}")
)

spark.stop()
EOF
        '
        """
        subprocess.run(["bash", "-lc", spark_cmd], check=True)

        # ---------------- Delete archived docs ----------------
        delete_res = coll.delete_many(query)
        client.close()

        # ---------------- Metadata ----------------
        metadata = {
            "run_id": run_id,
            "archived_at_utc": datetime.now(timezone.utc).isoformat(),
            "cutoff_timestamp": cutoff_str,
            "documents_archived": count,
            "documents_deleted": delete_res.deleted_count,
            "raw_hdfs_path": raw_hdfs_path,
            "parquet_base_path": HDFS_PARQUET_BASE,
            "policy": "30min archive, keep 1h hot data, raw->parquet per factory"
        }

        meta_json = json.dumps(metadata, indent=2)
        _hdfs(
            NAMENODE,
            f"echo '{meta_json}' | hdfs dfs -put -f - {meta_hdfs_path}"
        )

        return metadata

    archive_raw_sensor_data()
