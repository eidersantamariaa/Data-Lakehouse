#!/usr/bin/env python
"""
worker.py — proceso independiente que ejecuta operaciones concurrentes sobre Iceberg.
Lanzado por el notebook launcher. Cada worker recibe su configuración por argv.

Uso:
    python worker.py <worker_id> <operation> <target_filter>

    worker_id     : identificador numérico (0, 1, 2...)
    operation     : insert | delete | update | read
    target_filter : valor para filtrar/identificar filas (usado en update/delete)
"""

import sys
import time
import random
import json
import os
import glob

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

try:
    from py4j.protocol import Py4JJavaError
except ImportError:
    Py4JJavaError = Exception

# ── ARGS ──────────────────────────────────────────────────
worker_id     = int(sys.argv[1])
operation     = sys.argv[2]          # insert | delete | update | read
target_filter = sys.argv[3]          # valor de filtro o identificador

# ── CONFIG ───────────────────────────────────────────────── 
CATALOG  = "players"
DATABASE = "concurrency_tests"
TARGET   = f"{CATALOG}.{DATABASE}.concurrent_table"

BARRIER_READY = f"/tmp/worker_{worker_id}_ready"
BARRIER_GO    = "/tmp/all_workers_go"

NUM_ITERATIONS = 10   # cuántas operaciones hace cada worker
SLEEP_BASE     = 0.5  # segundos entre operaciones
SLEEP_JITTER   = 0.3  # variación aleatoria para desincronizar


def log(data: dict):
    """Emite una línea JSON por stdout — el notebook la parsea."""
    print(json.dumps(data), flush=True)


def log_info(msg: str):
    """Mensaje de texto plano — el notebook lo imprime como LOG."""
    print(f"INFO: {msg}", flush=True)


# ── SPARK SESSION ──────────────────────────────────────────
log_info(f"[W{worker_id}] Iniciando SparkSession...")

builder = SparkSession.builder.appName(f"worker_{worker_id}_{operation}")

builder = builder.config(
    "spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367"
)
builder = builder.config(
    "spark.jars.excludes",
    "org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api"
)
builder = builder.config(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)

# Iceberg REST catalog
builder = builder.config("spark.sql.catalog.players",           "org.apache.iceberg.spark.SparkCatalog")
builder = builder.config("spark.sql.catalog.players.type",      "rest")
builder = builder.config("spark.sql.catalog.players.uri",       "http://172.16.58.11:32688")
builder = builder.config("spark.sql.catalog.players.warehouse", "s3://warehouse/")
builder = builder.config("spark.sql.catalog.players.io-impl",   "org.apache.iceberg.aws.s3.S3FileIO")

# S3 / Garage
builder = builder.config("spark.sql.catalog.players.s3.endpoint",           "http://172.16.58.11:31224")
builder = builder.config("spark.sql.catalog.players.s3.region",             "us-east-1")
builder = builder.config("spark.sql.catalog.players.s3.path-style-access",  "true")
builder = builder.config("spark.sql.catalog.players.client.region",         "us-east-1")
builder = builder.config("spark.hadoop.fs.s3a.endpoint",                    "http://172.16.58.11:31224")
builder = builder.config("spark.hadoop.fs.s3a.endpoint.region",             "us-east-1")
builder = builder.config("spark.hadoop.fs.s3a.access.key",                  "GK5f421d5f440758f74b0e0312")
builder = builder.config("spark.hadoop.fs.s3a.secret.key",                  "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d")
builder = builder.config("spark.hadoop.fs.s3a.path.style.access",           "true")
builder = builder.config("spark.hadoop.fs.s3a.impl",                        "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Iceberg S3 tuning
builder = builder.config("spark.sql.catalog.players.s3.checksum-algorithm",       "NONE")
builder = builder.config("spark.sql.catalog.players.s3.streaming-upload-enabled", "false")
builder = builder.config("spark.sql.catalog.players.s3.chunked-encoding-enabled", "false")
builder = builder.config("spark.sql.catalog.players.s3.payload-signing-enabled",  "true")
builder = builder.config("spark.sql.catalog.players.s3.http-client-type",         "urlconnection")
builder = builder.config("spark.executor.extraJavaOptions", "-Daws.requestChecksumCalculation=when_required")
builder = builder.config("spark.driver.extraJavaOptions",   "-Daws.requestChecksumCalculation=when_required")
builder = builder.config("spark.sql.catalog.players.s3.access-key-id",     "GK5f421d5f440758f74b0e0312")
builder = builder.config("spark.sql.catalog.players.s3.secret-access-key", "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

log_info(f"[W{worker_id}] SparkSession lista")


# ── BARRERA DE SINCRONIZACIÓN ──────────────────────────────
# Señaliza que este worker está listo y espera al disparo del notebook
log_info(f"[W{worker_id}] Esperando barrera de inicio...")
open(BARRIER_READY, "w").close()

timeout = 120
t0 = time.time()
while not os.path.exists(BARRIER_GO):
    if time.time() - t0 > timeout:
        log_info(f"[W{worker_id}] ERROR: timeout esperando barrera")
        sys.exit(1)
    time.sleep(0.02)  # polling rápido — 20ms

log_info(f"[W{worker_id}] ¡GO! Arrancando operaciones ({operation})")


# ── OPERACIONES ────────────────────────────────────────────
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET} (
        id     INT,
        source STRING,
        value  STRING,
        ts     TIMESTAMP
    ) USING iceberg
    TBLPROPERTIES (
        'write.merge.mode'         = 'merge-on-read',
        'commit.retry.num-retries' = '4',
        'commit.retry.min-wait-ms' = '100'
    )
""")

schema = StructType([
    StructField("id",     IntegerType()),
    StructField("source", StringType()),
    StructField("value",  StringType()),
    StructField("ts",     TimestampType()),
])

for iteration in range(NUM_ITERATIONS):
    start_ns = time.time_ns()
    status   = "ok"
    error    = None
    rows     = 0

    try:
        if operation == "insert":
            base_id = worker_id * 1000 + iteration
            df = spark.createDataFrame(
                [(base_id, "seed", f"iter_{iteration}", datetime.now())],
                schema
            )
            df.writeTo(TARGET).append()
            rows = 1

        elif operation == "delete":
            spark.sql(f"DELETE FROM {TARGET} WHERE source = 'seed'")
            rows = 1

        elif operation == "update":
            spark.sql(f"""
                UPDATE {TARGET}
                SET value = 'updated_by_w{worker_id}_iter{iteration}'
                WHERE id = {target_filter}
            """)
            rows = 1

        elif operation == "read":
            rows = spark.table(TARGET).count()

    except Exception as e:
        err_str = str(e)
        if "CommitFailedException" in err_str or "ValidationException" in err_str:
            status = "conflict"
        else:
            status = "error"
        error = err_str[:200]

    end_ns = time.time_ns()

    log({
        "worker_id"   : worker_id,
        "operation"   : operation,
        "iteration"   : iteration,
        "target"      : target_filter,
        "status"      : status,
        "rows"        : rows,
        "duration_ms" : (end_ns - start_ns) / 1_000_000,
        "start_time"  : start_ns / 1e9,
        "end_time"    : end_ns / 1e9,
        "error"       : error,
    })

    time.sleep(SLEEP_BASE + random.uniform(0, SLEEP_JITTER))

log_info(f"[W{worker_id}] Terminado tras {NUM_ITERATIONS} iteraciones")
spark.stop()
