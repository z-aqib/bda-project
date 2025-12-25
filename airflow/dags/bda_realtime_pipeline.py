from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone
import os
import json
import uuid
import subprocess
import shutil
from pymongo import MongoClient


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _hdfs(container: str, cmd: str) -> str:
    """
    Runs HDFS command inside namenode container with correct PATH.
    Returns stdout (useful for debugging).
    """
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
        raise RuntimeError(
            f"HDFS command failed (rc={p.returncode})\nCMD: {cmd}\nSTDOUT:\n{p.stdout}\nSTDERR:\n{p.stderr}"
        )
    return (p.stdout or "").strip()


def _coll_size_bytes(mongo_uri: str, db: str, coll: str) -> int:
    client = MongoClient(mongo_uri)
    stats = client[db].command("collStats", coll)
    client.close()
    return int(stats.get("size", 0))


def _max_event_ts(client: MongoClient, db: str, coll: str):
    doc = client[db][coll].find({}, {"event_timestamp": 1}).sort("event_timestamp", -1).limit(1)
    lst = list(doc)
    return lst[0]["event_timestamp"] if lst else None


def _free_bytes(path: str) -> int:
    usage = shutil.disk_usage(path)
    return int(usage.free)


def _pipe_local_file_into_container(container: str, local_path: str, container_tmp_path: str) -> None:
    """
    Streams a local file into a container path without needing shared volumes.
    """
    # Use bash -lc so redirection works
    subprocess.run(
        ["bash", "-lc", f"cat '{local_path}' | /usr/bin/docker exec -i {container} bash -lc 'cat > {container_tmp_path}'"],
        check=True,
    )


with DAG(
    dag_id="bda_realtime_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
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

        # Safety knobs
        chunk_docs = int(_env("ARCHIVE_CHUNK_DOCS", "20000"))     # safer default
        min_free_mb = int(_env("ARCHIVE_MIN_FREE_MB", "800"))     # stop before disk issues
        max_chunks_per_run = int(_env("ARCHIVE_MAX_CHUNKS", "10"))  # keep each run short

        os.makedirs(local_dir, exist_ok=True)

        # 1) Check collection size
        size_bytes = _coll_size_bytes(mongo_uri, mongo_db, mongo_coll)
        size_mb = size_bytes / (1024 * 1024)

        if size_mb <= threshold_mb:
            return {"archived": False, "size_mb": round(size_mb, 2)}

        # 2) Free space guard (prevents Errno 28)
        free_mb = _free_bytes(local_dir) / (1024 * 1024)
        if free_mb < min_free_mb:
            return {
                "archived": False,
                "reason": "low_disk_space",
                "free_mb": round(free_mb, 2),
                "min_required_mb": min_free_mb,
            }

        client = MongoClient(mongo_uri)

        try:
            max_ts = _max_event_ts(client, mongo_db, mongo_coll)
            if not max_ts:
                return {"archived": False, "reason": "no_data"}

            # cutoff: keep newest keep_minutes
            max_dt = datetime.strptime(max_ts, "%Y-%m-%d %H:%M:%S")
            cutoff_dt = max_dt - timedelta(minutes=keep_minutes)
            cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

            q = {"event_timestamp": {"$lt": cutoff_str}}
            coll = client[mongo_db][mongo_coll]

            # Create HDFS dirs once
            _hdfs(hdfs_container, f"hdfs dfs -mkdir -p '{hdfs_archive_dir}'")
            _hdfs(hdfs_container, f"hdfs dfs -mkdir -p '{hdfs_metadata_dir}'")

            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
            total_archived = 0
            total_deleted = 0
            min_ts = None
            max_arch_ts = None
            parts_written = 0
            hdfs_files = []

            cursor = coll.find(q, no_cursor_timeout=True).batch_size(2000)

            try:
                buffer_ids = []
                part_no = 1

                data_file = None
                f = None

                def _start_part(pno: int):
                    nonlocal f, data_file
                    if f:
                        f.close()
                    data_file = os.path.join(local_dir, f"{mongo_db}_{mongo_coll}_{run_id}_part{pno:03d}.jsonl")
                    f = open(data_file, "w", encoding="utf-8")
                    return data_file

                _start_part(part_no)

                def _upload_and_delete_current_part():
                    nonlocal buffer_ids, data_file, parts_written, total_deleted

                    if not buffer_ids:
                        return

                    base = os.path.basename(data_file)
                    tmp_path = f"/tmp/{base}"
                    hdfs_path = f"{hdfs_archive_dir}/{base}"

                    # stream file into namenode:/tmp and put to HDFS
                    _pipe_local_file_into_container(hdfs_container, data_file, tmp_path)
                    _hdfs(hdfs_container, f"hdfs dfs -put -f '{tmp_path}' '{hdfs_path}'")

                    # delete docs by _id list
                    del_res = coll.delete_many({"_id": {"$in": buffer_ids}})
                    total_deleted += int(del_res.deleted_count)

                    # cleanup
                    try:
                        os.remove(data_file)
                    except FileNotFoundError:
                        pass
                    try:
                        _hdfs(hdfs_container, f"rm -f '{tmp_path}' || true")  # harmless
                    except Exception:
                        pass

                    hdfs_files.append(hdfs_path)
                    buffer_ids = []
                    parts_written += 1

                for doc in cursor:
                    # update min/max ts
                    ts = doc.get("event_timestamp")
                    if ts:
                        min_ts = ts if min_ts is None else min(min_ts, ts)
                        max_arch_ts = ts if max_arch_ts is None else max(max_arch_ts, ts)

                    # write json line
                    doc_id = doc.get("_id")
                    doc["_id"] = str(doc_id)
                    f.write(json.dumps(doc) + "\n")

                    buffer_ids.append(doc_id)
                    total_archived += 1

                    # rotate file when chunk reached
                    if len(buffer_ids) >= chunk_docs:
                        f.close()
                        _upload_and_delete_current_part()

                        if parts_written >= max_chunks_per_run:
                            break

                        part_no += 1
                        _start_part(part_no)

                if f:
                    f.close()

                # flush remaining
                if buffer_ids:
                    _upload_and_delete_current_part()

                if total_archived == 0:
                    return {"archived": False, "reason": "no_older_than_cutoff", "cutoff_str": cutoff_str, "size_mb": round(size_mb, 2)}

                # Metadata (small, safe)
                meta = {
                    "run_id": run_id,
                    "archived_at_utc": datetime.now(timezone.utc).isoformat(),
                    "mongo_db": mongo_db,
                    "mongo_collection": mongo_coll,
                    "policy": f"archive if size > {threshold_mb}MB; keep newest {keep_minutes} min; chunk={chunk_docs} docs/file; max_chunks_per_run={max_chunks_per_run}",
                    "cutoff_str": cutoff_str,
                    "docs_archived": total_archived,
                    "docs_deleted": total_deleted,
                    "min_event_timestamp": min_ts,
                    "max_event_timestamp": max_arch_ts,
                    "hdfs_data_paths": hdfs_files,
                }

                meta_file = os.path.join(local_dir, f"metadata_{run_id}.json")
                with open(meta_file, "w", encoding="utf-8") as mf:
                    json.dump(meta, mf, indent=2)

                meta_base = os.path.basename(meta_file)
                meta_tmp = f"/tmp/{meta_base}"
                hdfs_meta_path = f"{hdfs_metadata_dir}/{meta_base}"

                _pipe_local_file_into_container(hdfs_container, meta_file, meta_tmp)
                _hdfs(hdfs_container, f"hdfs dfs -put -f '{meta_tmp}' '{hdfs_meta_path}'")

                try:
                    os.remove(meta_file)
                except FileNotFoundError:
                    pass

                return {
                    "archived": True,
                    "size_mb_before": round(size_mb, 2),
                    "cutoff_str": cutoff_str,
                    "docs_archived": total_archived,
                    "docs_deleted": total_deleted,
                    "hdfs_parts_written": parts_written,
                    "hdfs_meta_path": hdfs_meta_path,
                }

            finally:
                try:
                    cursor.close()
                except Exception:
                    pass

        finally:
            client.close()

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

    archive_mongo_if_over_300mb() >> run_spark_kpis
