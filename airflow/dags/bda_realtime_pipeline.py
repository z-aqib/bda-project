from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone
import os
import json
import uuid
import subprocess

from pymongo import MongoClient


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _docker_exec(container: str, cmd: list[str]) -> None:
    # Uses /usr/bin/docker (as you requested earlier)
    subprocess.run(
        ["/usr/bin/docker", "exec", "-i", container] + cmd,
        check=True,
    )


def _coll_size_bytes(mongo_uri: str, db: str, coll: str) -> int:
    client = MongoClient(mongo_uri)
    stats = client[db].command("collStats", coll)
    client.close()
    # stats["size"] is collection data size in bytes (not including indexes)
    return int(stats.get("size", 0))


def _max_event_ts(client: MongoClient, db: str, coll: str):
    doc = client[db][coll].find({}, {"event_timestamp": 1}).sort("event_timestamp", -1).limit(1)
    lst = list(doc)
    return lst[0]["event_timestamp"] if lst else None


def _parse_ts(ts):
    # Your data is like "2025-12-25 17:54:12" (string)
    # Keep it simple: treat as naive, compare lexicographically (works for YYYY-MM-DD HH:MM:SS)
    return ts


with DAG(
    dag_id="bda_realtime_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",  # every minute (matches requirement)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["bda", "realtime", "archive", "spark"],
):

    @task
    def archive_mongo_if_over_300mb():
        mongo_uri = _env("MONGO_URI", "mongodb://mongo:27017")
        mongo_db = _env("MONGO_DB", "iot_db")
        mongo_coll = _env("MONGO_COLLECTION", "sensor_readings")

        threshold_mb = int(_env("ARCHIVE_THRESHOLD_MB", "300"))
        keep_minutes = int(_env("ARCHIVE_KEEP_MINUTES", "15"))

        local_dir = _env("LOCAL_ARCHIVE_DIR", "/opt/airflow/archive")
        hdfs_container = _env("HDFS_CONTAINER", "namenode")
        hdfs_archive_dir = _env("HDFS_ARCHIVE_DIR", "/bda/archive/mongo")
        hdfs_metadata_dir = _env("HDFS_METADATA_DIR", "/bda/archive/metadata")

        os.makedirs(local_dir, exist_ok=True)

        size_bytes = _coll_size_bytes(mongo_uri, mongo_db, mongo_coll)
        size_mb = size_bytes / (1024 * 1024)

        if size_mb <= threshold_mb:
            return {"archived": False, "size_mb": round(size_mb, 2)}

        # Connect and decide cutoff: keep newest N minutes, archive older
        client = MongoClient(mongo_uri)
        max_ts = _max_event_ts(client, mongo_db, mongo_coll)
        if not max_ts:
            client.close()
            return {"archived": False, "reason": "no_data"}

        # event_timestamp is a sortable string; we compute a cutoff string by parsing to datetime
        # If your timestamps are always in "%Y-%m-%d %H:%M:%S", this works.
        max_dt = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
        cutoff_dt = max_dt - timedelta(minutes=keep_minutes)
        cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

        # Pull older docs
        q = {"event_timestamp": {"$lt": cutoff_str}}
        cursor = client[mongo_db][mongo_coll].find(q)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
        data_file = os.path.join(local_dir, f"{mongo_db}_{mongo_coll}_{run_id}.jsonl")
        meta_file = os.path.join(local_dir, f"metadata_{run_id}.json")

        count = 0
        min_ts = None
        max_arch_ts = None

        with open(data_file, "w", encoding="utf-8") as f:
            for doc in cursor:
                # remove ObjectId for JSON
                doc["_id"] = str(doc["_id"])
                ts = doc.get("event_timestamp")
                if ts:
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                    max_arch_ts = ts if max_arch_ts is None else max(max_arch_ts, ts)
                f.write(json.dumps(doc) + "\n")
                count += 1

        if count == 0:
            client.close()
            # No older rows to archive yet (even though size says big)
            return {"archived": False, "size_mb": round(size_mb, 2), "reason": "no_older_than_cutoff"}

        # HDFS put
        # 1) mkdir
        _docker_exec(hdfs_container, ["hdfs", "dfs", "-mkdir", "-p", hdfs_archive_dir])
        _docker_exec(hdfs_container, ["hdfs", "dfs", "-mkdir", "-p", hdfs_metadata_dir])

        hdfs_data_path = f"{hdfs_archive_dir}/{os.path.basename(data_file)}"
        hdfs_meta_path = f"{hdfs_metadata_dir}/{os.path.basename(meta_file)}"

        # 2) copy file into hdfs container then put to HDFS (simplest with docker exec + cat)
        # Put data
        _docker_exec(
            hdfs_container,
            ["bash", "-lc", f"cat > /tmp/{os.path.basename(data_file)}"],
        )
        subprocess.run(
            ["bash", "-lc", f"cat {data_file} | /usr/bin/docker exec -i {hdfs_container} bash -lc 'cat > /tmp/{os.path.basename(data_file)}'"],
            check=True,
        )
        _docker_exec(hdfs_container, ["hdfs", "dfs", "-put", "-f", f"/tmp/{os.path.basename(data_file)}", hdfs_data_path])

        # Metadata
        metadata = {
            "run_id": run_id,
            "archived_at_utc": datetime.now(timezone.utc).isoformat(),
            "mongo_db": mongo_db,
            "mongo_collection": mongo_coll,
            "policy": f"archive if size > {threshold_mb}MB; keep newest; archived older than {keep_minutes} minutes",
            "cutoff_str": cutoff_str,
            "docs_archived": count,
            "min_event_timestamp": min_ts,
            "max_event_timestamp": max_arch_ts,
            "hdfs_data_path": hdfs_data_path,
        }
        with open(meta_file, "w", encoding="utf-8") as mf:
            json.dump(metadata, mf, indent=2)

        subprocess.run(
            ["bash", "-lc", f"cat {meta_file} | /usr/bin/docker exec -i {hdfs_container} bash -lc 'cat > /tmp/{os.path.basename(meta_file)}'"],
            check=True,
        )
        _docker_exec(hdfs_container, ["hdfs", "dfs", "-put", "-f", f"/tmp/{os.path.basename(meta_file)}", hdfs_meta_path])

        # Delete archived docs from Mongo (older data only)
        delete_res = client[mongo_db][mongo_coll].delete_many(q)
        client.close()

        return {
            "archived": True,
            "size_mb_before": round(size_mb, 2),
            "docs_archived": count,
            "docs_deleted": int(delete_res.deleted_count),
            "cutoff_str": cutoff_str,
            "hdfs_data_path": hdfs_data_path,
            "hdfs_meta_path": hdfs_meta_path,
        }

    # Runs your Spark command INSIDE the spark container via /usr/bin/docker exec
    run_spark_kpis = BashOperator(
        task_id="spark_kpi_to_postgres",
        bash_command=r"""
        set -e
        /usr/bin/docker exec -i ${SPARK_CONTAINER:-spark} bash -lc '
          export PATH=$PATH:/opt/spark/bin
          rm -rf /tmp/spark-local && mkdir -p /tmp/spark-local && chmod 777 /tmp/spark-local
          SPARK_LOCAL_DIRS=/tmp/spark-local spark-submit \
            --master local[1] \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --conf spark.sql.shuffle.partitions=1 \
            --conf spark.default.parallelism=1 \
            --conf spark.sql.adaptive.enabled=false \
            --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.2,org.postgresql:postgresql:42.7.3 \
            ${SPARK_APP_PATH:-/app/spark_analytics_dashboard.py}
        '
        """,
    )

    # Order: archive check (may do nothing) -> spark KPIs every minute
    archive_mongo_if_over_300mb() >> run_spark_kpis
