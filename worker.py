#!/usr/bin/env python
"""
worker.py — proceso de larga vida.

Inicializa Spark UNA sola vez y luego espera tareas del notebook.
Cada tarea llega como fichero JSON, el worker la ejecuta y escribe el resultado.

Protocolo de ficheros:
  /tmp/worker_{id}_ready          → worker señaliza que Spark está listo
  /tmp/worker_{id}_task.json      → notebook escribe la tarea
  /tmp/worker_{id}_result.json    → worker escribe el resultado
  /tmp/worker_{id}_result_done    → worker señaliza que el resultado está listo

Formato tarea:
  {"operation": "update", "target": "7", "scenario": "s08_upd_upd", "iteration": 0}
  {"operation": "stop"}   ← termina el proceso
"""

import sys, time, random, json, os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

worker_id = int(sys.argv[1])

CATALOG = "players"
DATABASE = "concurrency_tests"
TARGET  = f"{CATALOG}.{DATABASE}.concurrent_table"

TASK_FILE   = f"/tmp/worker_{worker_id}_task.json"
RESULT_FILE = f"/tmp/worker_{worker_id}_result.json"
RESULT_DONE = f"/tmp/worker_{worker_id}_result_done"
READY_FILE  = f"/tmp/worker_{worker_id}_ready"

def info(msg): print(f"INFO: {msg}", flush=True)

# ── SPARK — se inicializa una sola vez ──────────────────────
info(f"[W{worker_id}] Iniciando SparkSession (solo una vez)...")

builder = SparkSession.builder.appName(f"persistent_worker_{worker_id}")
builder = builder.config("spark.jars.packages",
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
    "org.apache.hadoop:hadoop-aws:3.4.2,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367")
builder = builder.config("spark.jars.excludes",
    "org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api")
builder = builder.config("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
for k, v in [
    ("spark.sql.catalog.players",                            "org.apache.iceberg.spark.SparkCatalog"),
    ("spark.sql.catalog.players.type",                       "rest"),
    ("spark.sql.catalog.players.uri",                        "http://172.16.58.11:32688"),
    ("spark.sql.catalog.players.warehouse",                  "s3://warehouse/"),
    ("spark.sql.catalog.players.io-impl",                    "org.apache.iceberg.aws.s3.S3FileIO"),
    ("spark.sql.catalog.players.s3.endpoint",                "http://172.16.58.11:31224"),
    ("spark.sql.catalog.players.s3.region",                  "us-east-1"),
    ("spark.sql.catalog.players.s3.path-style-access",       "true"),
    ("spark.sql.catalog.players.client.region",              "us-east-1"),
    ("spark.hadoop.fs.s3a.endpoint",                         "http://172.16.58.11:31224"),
    ("spark.hadoop.fs.s3a.endpoint.region",                  "us-east-1"),
    ("spark.hadoop.fs.s3a.access.key",                       "GK5f421d5f440758f74b0e0312"),
    ("spark.hadoop.fs.s3a.secret.key",                       "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d"),
    ("spark.hadoop.fs.s3a.path.style.access",                "true"),
    ("spark.hadoop.fs.s3a.impl",                             "org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ("spark.sql.catalog.players.s3.checksum-algorithm",      "NONE"),
    ("spark.sql.catalog.players.s3.streaming-upload-enabled","false"),
    ("spark.sql.catalog.players.s3.chunked-encoding-enabled","false"),
    ("spark.sql.catalog.players.s3.payload-signing-enabled", "true"),
    ("spark.sql.catalog.players.s3.http-client-type",        "urlconnection"),
    ("spark.executor.extraJavaOptions",                      "-Daws.requestChecksumCalculation=when_required"),
    ("spark.driver.extraJavaOptions",                        "-Daws.requestChecksumCalculation=when_required"),
    ("spark.sql.catalog.players.s3.access-key-id",          "GK5f421d5f440758f74b0e0312"),
    ("spark.sql.catalog.players.s3.secret-access-key",      "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d"),
]:
    builder = builder.config(k, v)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
info(f"[W{worker_id}] SparkSession lista — esperando tareas")

schema = StructType([
    StructField("id",     IntegerType()),
    StructField("source", StringType()),
    StructField("value",  StringType()),
    StructField("ts",     TimestampType()),
])

# Señaliza al notebook que está listo
open(READY_FILE, "w").close()

# ── BUCLE DE TAREAS ─────────────────────────────────────────
while True:
    # Espera a que el notebook escriba una tarea
    while not os.path.exists(TASK_FILE):
        time.sleep(0.02)

    # Lee y borra la tarea inmediatamente
    with open(TASK_FILE) as f:
        task = json.load(f)
    os.remove(TASK_FILE)

    # Señal de parada
    if task.get("operation") == "stop":
        info(f"[W{worker_id}] Stop recibido — cerrando")
        break

    operation = task["operation"]
    target    = str(task.get("target", "0"))
    scenario  = task.get("scenario", "?")
    iteration = task.get("iteration", 0)

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
                WHERE id = {target}
            """)
            rows = 1

        elif operation == "delete":
            spark.sql(f"DELETE FROM {TARGET} WHERE id = {target}")
            rows = 1

        elif operation == "merge":
            spark.createDataFrame(
                [(int(target), f"worker_{worker_id}", f"merge_w{worker_id}_i{iteration}", datetime.now())],
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
        if   "CommitFailedException" in err_str: status = "commit_conflict"
        elif "ValidationException"   in err_str: status = "validation_conflict"
        else:                                     status = "error"
        error = err_str[:300]

    end_ns = time.time_ns()

    result = {
        "worker_id"   : worker_id,
        "operation"   : operation,
        "iteration"   : iteration,
        "target"      : target,
        "scenario"    : scenario,
        "status"      : status,
        "rows"        : rows,
        "duration_ms" : (end_ns - start_ns) / 1_000_000,
        "start_time"  : start_ns / 1e9,
        "end_time"    : end_ns / 1e9,
        "error"       : error,
    }

    # Escribe resultado y señaliza que está listo
    with open(RESULT_FILE, "w") as f:
        json.dump(result, f)
    open(RESULT_DONE, "w").close()

spark.stop()
info(f"[W{worker_id}] Proceso terminado")