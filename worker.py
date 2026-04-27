#!/usr/bin/env python
"""
worker.py — proceso independiente para tests de concurrencia sobre Iceberg.

Uso:
    python worker.py <worker_id> <operation> <target_filter> <scenario_id>

    worker_id     : identificador numérico (0, 1...)
    operation     : insert | update | delete | merge | read
    target_filter : id de fila a tocar
    scenario_id   : nombre del escenario (barreras independientes por test)
"""

import sys, time, random, json, os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

# ── ARGS ───────────────────────────────────────────────────
worker_id     = int(sys.argv[1])
operation     = sys.argv[2]
target_filter = sys.argv[3]
scenario_id   = sys.argv[4] if len(sys.argv) > 4 else "default"

# ── CONFIG ─────────────────────────────────────────────────
CATALOG  = "players"
DATABASE = "concurrency_tests"
TARGET   = f"{CATALOG}.{DATABASE}.concurrent_table"

# Barreras únicas por escenario — evita interferencias entre tests
BARRIER_READY = f"/tmp/{scenario_id}_worker_{worker_id}_ready"
BARRIER_GO    = f"/tmp/{scenario_id}_go"

NUM_ITERATIONS = 5
SLEEP_JITTER   = 0.2   # sin base — máxima presión sobre el catálogo

def log(data):      print(json.dumps(data), flush=True)
def info(msg):      print(f"INFO: {msg}", flush=True)

# ── SPARK ───────────────────────────────────────────────────
info(f"[W{worker_id}] Iniciando SparkSession para '{scenario_id}'...")

builder = SparkSession.builder.appName(f"{scenario_id}_w{worker_id}_{operation}")
builder = builder.config("spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367")
builder = builder.config("spark.jars.excludes",
    "org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api")
builder = builder.config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
builder = builder.config("spark.sql.catalog.players",            "org.apache.iceberg.spark.SparkCatalog")
builder = builder.config("spark.sql.catalog.players.type",       "rest")
builder = builder.config("spark.sql.catalog.players.uri",        "http://172.16.58.11:32688")
builder = builder.config("spark.sql.catalog.players.warehouse",  "s3://warehouse/")
builder = builder.config("spark.sql.catalog.players.io-impl",    "org.apache.iceberg.aws.s3.S3FileIO")
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
info(f"[W{worker_id}] SparkSession lista")

# ── BARRERA ─────────────────────────────────────────────────
info(f"[W{worker_id}] Esperando barrera '{scenario_id}'...")
open(BARRIER_READY, "w").close()
t0 = time.time()
while not os.path.exists(BARRIER_GO):
    if time.time() - t0 > 180:
        info(f"[W{worker_id}] ERROR: timeout en barrera"); sys.exit(1)
    time.sleep(0.02)
info(f"[W{worker_id}] ¡GO! ({operation}, target={target_filter})")

# ── OPERACIONES ─────────────────────────────────────────────
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
    status = "ok"
    error  = None
    rows   = 0

    try:
        if operation == "insert":
            base_id = 10000 + worker_id * 1000 + iteration
            spark.createDataFrame(
                [(base_id, f"worker_{worker_id}", f"iter_{iteration}", datetime.now())], schema
            ).writeTo(TARGET).append()
            rows = 1

        elif operation == "update":
            spark.sql(f"""
                UPDATE {TARGET}
                SET value = 'upd_w{worker_id}_i{iteration}', source = 'worker_{worker_id}'
                WHERE id = {target_filter}
            """)
            rows = 1

        elif operation == "delete":
            spark.sql(f"DELETE FROM {TARGET} WHERE id = {target_filter}")
            rows = 1

        elif operation == "merge":
            spark.createDataFrame(
                [(int(target_filter), f"worker_{worker_id}", f"merge_w{worker_id}_i{iteration}", datetime.now())],
                schema
            ).createOrReplaceTempView("merge_source")
            spark.sql(f"""
                MERGE INTO {TARGET} t
                USING merge_source s ON t.id = s.id
                WHEN MATCHED THEN
                    UPDATE SET t.value = s.value, t.source = s.source, t.ts = s.ts
                WHEN NOT MATCHED THEN
                    INSERT (id, source, value, ts) VALUES (s.id, s.source, s.value, s.ts)
            """)
            rows = 1

        elif operation == "read":
            spark.catalog.refreshTable(TARGET)
            rows = spark.table(TARGET).count()

    except Exception as e:
        err_str = str(e)
        if   "CommitFailedException"  in err_str: status = "commit_conflict"
        elif "ValidationException"    in err_str: status = "validation_conflict"
        else:                                      status = "error"
        error = err_str[:300]

    end_ns = time.time_ns()
    log({
        "worker_id"   : worker_id,
        "operation"   : operation,
        "iteration"   : iteration,
        "target"      : target_filter,
        "scenario"    : scenario_id,
        "status"      : status,
        "rows"        : rows,
        "duration_ms" : (end_ns - start_ns) / 1_000_000,
        "start_time"  : start_ns / 1e9,
        "end_time"    : end_ns / 1e9,
        "error"       : error,
    })

    time.sleep(SLEEP_JITTER * random.random())

info(f"[W{worker_id}] Terminado")
spark.stop()